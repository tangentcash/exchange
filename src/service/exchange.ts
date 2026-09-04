import { BigNumber } from "bignumber.js";
import { AssetId, ByteUtil, Hashing, Pubkeyhash, Readability, Signing, Spot, Uint256, Whitelist } from 'tangentsdk';
import { MarketPolicy, Market, Order, OrderCondition, OrderPolicy, OrderSide, Trade, AggregatedPair, AggregatedTrade, AggregatedLevel, AggregatedLog, Block, RefType, Pool, Depth, Delegator, DelegatedPool, PseudoDelegatedPool, PseudoDelegatedState } from './../types';
import { Log } from './../logging';
import { Common } from './../common';
import { Blockchain, EventInfo } from './blockchain';
import { Quotes, symbolOf } from './market';
import NodeCache from 'node-cache';
import pq from 'postgres';
import os from 'os';

const MARKET_CLEANUP = false;

export type PriceDescriptors = Record<string, { whitelist: boolean, base: string | null, price: { open: BigNumber | null, close: BigNumber | null } }>;

export type Connection = pq.TransactionSql;

export enum Notification {
    AuthorizerResponse = 'response:authorizer',
    ChainUpdate = 'update:chain',
    MarketUpdate = 'update:market',
    DelegatorUpdate = 'update:delegator',
    DelegatedPoolUpdate = 'update:delegated-pool',
    OrderUpdate = 'update:order',
    PoolUpdate = 'update:pool',
    TradeUpdate = 'update:trade',
    LevelUpdate = 'update:level'
}

export type Pair = {
    id: Uint256,
    primaryAsset: { id: Uint256, hash: AssetId } | null,
    secondaryAsset: { id: Uint256, hash: AssetId } | null
};

export type RouterPath = {
    pair: Pair,
    side: OrderSide,
    input: { min: BigNumber, max: BigNumber },
    output: { min: BigNumber, max: BigNumber }
}[];

export type PseudoOrder = {
    transaction: EventInfo,
    primaryAsset: AssetId,
    secondaryAsset: AssetId,
    condition: OrderCondition;
    side: OrderSide;
    policy: OrderPolicy;
    price?: BigNumber;
    stopPrice?: BigNumber;
    value: BigNumber;
    slippage?: BigNumber;
    trailingStep?: BigNumber;
    trailingDistance?: BigNumber;
}

export type PseudoPool = {
    transaction: EventInfo,
    primaryAsset: AssetId,
    secondaryAsset: AssetId,
    price: BigNumber;
    minPrice?: BigNumber;
    maxPrice?: BigNumber;
    feeRate: BigNumber;
}

export type Options = {
    host?: string;
    port?: number;
    user?: string;
    password?: string;
    database?: string;
    application?: string;
    connections?: number;
}

export type Caches = {
    assetIdToPolyAssetId: NodeCache;
    assetHashToAssetId: NodeCache;
    assetIdsToPairId: NodeCache;
    assetPairIdToAssets: NodeCache;
    accountHashToAssetId: NodeCache;
}

export class Cursor {
    offset: number = 0;
    count: number = 0;

    static offset(offset: number, count: number = 32): Cursor {
        const result = new Cursor();
        result.offset = offset;
        result.count = count;
        return result;
    }
    static page(number: number, count: number = 32): Cursor {
        const result = new Cursor();
        result.count = count;
        result.offset = result.count * number;
        return result;
    }
}

export class TimeCursor {
    interval: number = 0;
    fromTime: number = 0;
    toTime: number = 0;
    maxTime: number = 0;
    page: number = 0;

    static page(interval: number, number: number, count: number = 512): TimeCursor {
        const result = new TimeCursor();
        const time = new Date().getTime();
        result.interval = interval;
        result.maxTime = time + interval - time % interval;
        result.fromTime = interval * number;
        result.toTime = Math.min(interval * (number + count), result.maxTime);
        result.page = number;
        return result;
    }
}

export class Exchange {
    static listeners: Record<string, pq.ListenMeta> = { };
    static connection: pq.Sql;
    static cache: Caches;

    static clearCache(): void {
        const options = { maxKeys: 1024 * 8 };
        this.cache = {
            assetIdToPolyAssetId: new NodeCache(options),
            assetHashToAssetId: new NodeCache(options),
            assetIdsToPairId: new NodeCache(options),
            assetPairIdToAssets: new NodeCache(options),
            accountHashToAssetId: new NodeCache(options)
        };
    }
    static async setup(config: Options): Promise<void> {
        const sql = pq({
            host: config.host || 'localhost',
            port: config.port || 19419,
            user: config.user,
            pass: config.password,
            database: config.database || 'ts',
            max: config.connections || Math.max(4, os.cpus().length),
            connection: {
                application_name: config.application || 'ts',
            },
            onnotice: (notice: pq.Notice) => {
                Log.info('storage notice:', notice['message'] || notice);
            },
            debug: (connection: number, query: string, parameters: any[], _: any[]) => {
                Log.query(`storage query (parameters: ${parameters}, connections: ${connection}):\n`, query);
            }
        });
 
        const result = await this.resultOf(sql`SELECT COUNT(1) FROM pg_extension WHERE extname = 'timescaledb'`);
        if (result[0]['count'] < 1)
            throw new Error('postgresql database must have \'timescaledb\' extension loaded');

        this.connection = sql;
        this.clearCache();
        return this.deploy();
    }
    static async shutdown(): Promise<void> {
        await this.connection.end({ timeout: 5 });
    }
    static async deploy(connection?: pq.TransactionSql): Promise<void> {
        const sql = connection || this.connection;
        await this.resultOf(sql`
        CREATE TABLE IF NOT EXISTS blocks
        (
            block_number BIGSERIAL,
            block_hash BYTEA NOT NULL UNIQUE,
            PRIMARY KEY (block_number)
        );

        CREATE TABLE IF NOT EXISTS accounts
        (
            id BIGSERIAL,
            hash BYTEA NOT NULL UNIQUE,
            synced BOOLEAN DEFAULT FALSE,
            auto_time BIGINT DEFAULT floor(EXTRACT(EPOCH FROM CURRENT_TIMESTAMP)),
            PRIMARY KEY (id)
        );
        CREATE INDEX IF NOT EXISTS accounts_hash ON accounts USING hash (hash);

        CREATE TABLE IF NOT EXISTS assets
        (
            id BIGSERIAL,
            hash BYTEA NOT NULL UNIQUE,
            auto_time BIGINT DEFAULT floor(EXTRACT(EPOCH FROM CURRENT_TIMESTAMP)),
            PRIMARY KEY (id)
        );
        CREATE INDEX IF NOT EXISTS assets_hash ON assets USING hash (hash);
        
        CREATE TABLE IF NOT EXISTS pairs
        (
            id BIGSERIAL,
            primary_asset_id BIGINT REFERENCES assets (id) ON DELETE CASCADE,
            secondary_asset_id BIGINT REFERENCES assets (id) ON DELETE CASCADE,
            launch_time BIGINT DEFAULT NULL,
            PRIMARY KEY (id),
            UNIQUE (primary_asset_id, secondary_asset_id)
        );

        CREATE TABLE IF NOT EXISTS balances
        (
            account_id BIGINT NOT NULL REFERENCES accounts (id) ON DELETE CASCADE,
            asset_id BIGINT NOT NULL REFERENCES assets (id) ON DELETE CASCADE,
            block_number BIGINT NOT NULL REFERENCES blocks (block_number) ON DELETE CASCADE,
            time BIGINT NOT NULL,
            value NUMERIC(96, 18) NOT NULL,
            PRIMARY KEY (account_id, asset_id)
        );
        
        CREATE TABLE IF NOT EXISTS markets
        (
            id BIGSERIAL,
            account_id BIGINT NOT NULL UNIQUE REFERENCES accounts (id) ON DELETE CASCADE,
            deployer_account_id BIGINT NOT NULL REFERENCES accounts (id) ON DELETE CASCADE,
            block_number BIGINT NOT NULL REFERENCES blocks (block_number) ON DELETE CASCADE,
            pool_exit_fee NUMERIC(96, 18) NOT NULL,
            max_pool_fee_rate NUMERIC(96, 18) NOT NULL,
            min_maker_fee NUMERIC(96, 18) NOT NULL,
            max_maker_fee NUMERIC(96, 18) NOT NULL,
            maker_fee_exponent BIGINT NOT NULL,
            min_taker_fee NUMERIC(96, 18) NOT NULL,
            max_taker_fee NUMERIC(96, 18) NOT NULL,
            taker_fee_exponent BIGINT NOT NULL,
            asset_volume_target NUMERIC(96, 18) NOT NULL,
            asset_reset_days BIGINT NOT NULL,
            account_reset_days BIGINT NOT NULL,
            market_policy SMALLINT NOT NULL,
            PRIMARY KEY (id)
        );

        CREATE TABLE IF NOT EXISTS delegators
        (
            id BIGSERIAL,
            market_id BIGINT NOT NULL REFERENCES markets (id) ON DELETE CASCADE,
            account_id BIGINT NOT NULL UNIQUE REFERENCES accounts (id) ON DELETE CASCADE,
            deployer_account_id BIGINT NOT NULL REFERENCES accounts (id) ON DELETE CASCADE,
            block_number BIGINT NOT NULL REFERENCES blocks (block_number) ON DELETE CASCADE,
            reward_emission NUMERIC(96, 18) NOT NULL,
            reward_balance NUMERIC(96, 18) NOT NULL,
            permissions JSONB DEFAULT NULL,
            PRIMARY KEY (id)
        );

        CREATE TABLE IF NOT EXISTS poly_assets
        (
            asset_id BIGINT NOT NULL REFERENCES assets (id) ON DELETE CASCADE,
            market_id BIGINT NOT NULL REFERENCES markets (id) ON DELETE CASCADE,
            poly_asset_id BIGINT NOT NULL REFERENCES assets (id) ON DELETE CASCADE,
            PRIMARY KEY (asset_id, market_id)
        );
        
        CREATE TABLE IF NOT EXISTS tiers
        (
            account_id BIGINT NOT NULL REFERENCES accounts (id) ON DELETE CASCADE,
            asset_id BIGINT NOT NULL REFERENCES assets (id) ON DELETE CASCADE,
            market_id BIGINT NOT NULL REFERENCES markets (id) ON DELETE CASCADE,
            block_number BIGINT NOT NULL REFERENCES blocks (block_number) ON DELETE CASCADE,
            volume NUMERIC(96, 18) NOT NULL,
            maker_fee NUMERIC(96, 18) NOT NULL,
            taker_fee NUMERIC(96, 18) NOT NULL,
            PRIMARY KEY (account_id, asset_id, market_id)
        );

        CREATE TABLE IF NOT EXISTS orders
        (
            id BIGSERIAL,
            order_id BYTEA NOT NULL,
            pair_id BIGINT NOT NULL REFERENCES pairs (id) ON DELETE CASCADE,
            market_id BIGINT NOT NULL REFERENCES markets (id) ON DELETE CASCADE,
            account_id BIGINT NOT NULL REFERENCES accounts (id) ON DELETE CASCADE,
            block_number BIGINT NOT NULL REFERENCES blocks (block_number) ON DELETE CASCADE,
            condition SMALLINT NOT NULL,
            side SMALLINT NOT NULL,
            policy SMALLINT NOT NULL,
            price NUMERIC(96, 18) DEFAULT NULL,
            stop_price NUMERIC(96, 18) DEFAULT NULL,
            filling_price NUMERIC(96, 18) DEFAULT NULL,
            starting_value NUMERIC(96, 18) NOT NULL,
            value NUMERIC(96, 18) NOT NULL,
            slippage NUMERIC(96, 18) DEFAULT NULL,
            trailing_step NUMERIC(96, 18) DEFAULT NULL,
            trailing_distance NUMERIC(96, 18) DEFAULT NULL,
            active BOOLEAN NOT NULL,
            last_price NUMERIC(96, 18) GENERATED ALWAYS AS (COALESCE(stop_price, price, filling_price, 0.0)) STORED,
            last_quantity NUMERIC(96, 18) GENERATED ALWAYS AS (CASE WHEN side = 0 THEN COALESCE(value / COALESCE(stop_price, price, filling_price), 0.0) ELSE value END) STORED,
            PRIMARY KEY (id),
            UNIQUE (market_id, order_id)
        );
        CREATE INDEX IF NOT EXISTS orders_order_id ON orders USING hash (order_id);
        
        CREATE TABLE IF NOT EXISTS pools
        (
            id BIGSERIAL,
            pool_id BYTEA NOT NULL,
            pair_id BIGINT NOT NULL REFERENCES pairs (id) ON DELETE CASCADE,
            market_id BIGINT NOT NULL REFERENCES markets (id) ON DELETE CASCADE,
            account_id BIGINT NOT NULL REFERENCES accounts (id) ON DELETE CASCADE,
            block_number BIGINT NOT NULL REFERENCES blocks (block_number) ON DELETE CASCADE,
            initial_price NUMERIC(96, 18) NOT NULL,
            initial_primary_value NUMERIC(96, 18) NOT NULL,
            initial_secondary_value NUMERIC(96, 18) NOT NULL,
            primary_value NUMERIC(96, 18) NOT NULL,
            secondary_value NUMERIC(96, 18) NOT NULL,
            primary_revenue NUMERIC(96, 18) NOT NULL,
            secondary_revenue NUMERIC(96, 18) NOT NULL,
            liquidity NUMERIC(96, 18) NOT NULL,
            price NUMERIC(96, 18) NOT NULL,
            min_price NUMERIC(96, 18) DEFAULT NULL,
            max_price NUMERIC(96, 18) DEFAULT NULL,
            fee_rate NUMERIC(96, 18) NOT NULL,
            exit_fee NUMERIC(96, 18) NOT NULL,
            active BOOLEAN NOT NULL,
            last_ask_price NUMERIC(96, 18) GENERATED ALWAYS AS (CASE WHEN active THEN COALESCE(price * (1 + fee_rate), 0.0) ELSE 0.0 END) STORED,
            last_bid_price NUMERIC(96, 18) GENERATED ALWAYS AS (CASE WHEN active THEN COALESCE(price * (1 - fee_rate), 0.0) ELSE 0.0 END) STORED,
            PRIMARY KEY (id),
            UNIQUE (market_id, pool_id)
        );
        CREATE INDEX IF NOT EXISTS pools_pool_id ON pools USING hash (pool_id);

        CREATE TABLE IF NOT EXISTS delegated_pools
        (
            id BIGSERIAL,
            pair_id BIGINT NOT NULL REFERENCES pairs (id) ON DELETE CASCADE,
            market_id BIGINT NOT NULL REFERENCES markets (id) ON DELETE CASCADE,
            delegator_id BIGINT NOT NULL REFERENCES delegators (id) ON DELETE CASCADE,
            account_id BIGINT NOT NULL REFERENCES accounts (id) ON DELETE CASCADE,
            block_number BIGINT NOT NULL REFERENCES blocks (block_number) ON DELETE CASCADE,
            reward_value NUMERIC(96, 18) NOT NULL,
            initial_primary_value NUMERIC(96, 18) NOT NULL,
            initial_secondary_value NUMERIC(96, 18) NOT NULL,
            primary_value NUMERIC(96, 18) NOT NULL,
            secondary_value NUMERIC(96, 18) NOT NULL,
            active BOOLEAN NOT NULL,
            PRIMARY KEY (id),
            UNIQUE (pair_id, market_id, delegator_id, account_id)
        );

        CREATE TABLE IF NOT EXISTS depths
        (
            pair_id BIGINT NOT NULL REFERENCES pairs (id) ON DELETE CASCADE,
            market_id BIGINT NOT NULL REFERENCES markets (id) ON DELETE CASCADE,
            pool_id BIGINT NOT NULL REFERENCES pools (id) ON DELETE CASCADE,
            account_id BIGINT NOT NULL REFERENCES accounts (id) ON DELETE CASCADE,
            block_number BIGINT NOT NULL REFERENCES blocks (block_number) ON DELETE CASCADE,
            price NUMERIC(96, 18) NOT NULL,
            quantity NUMERIC(96, 18) NOT NULL,
            time BIGINT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS depths_pair_id_time ON depths (pair_id, time DESC);
        
        CREATE TABLE IF NOT EXISTS trades
        (
            pair_id BIGINT NOT NULL,
            market_id BIGINT DEFAULT NULL,
            maker_order_id BIGINT DEFAULT NULL,
            maker_pool_id BIGINT DEFAULT NULL,
            maker_account_id BIGINT DEFAULT NULL,
            taker_order_id BIGINT DEFAULT NULL,
            taker_account_id BIGINT DEFAULT NULL,
            block_number BIGINT DEFAULT NULL,
            side SMALLINT NOT NULL,
            price NUMERIC(96, 18) NOT NULL,
            quantity NUMERIC(96, 18) NOT NULL,
            time BIGINT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS trades_pair_id_time ON trades (pair_id, time DESC);
        CREATE OR REPLACE FUNCTION system_clock() RETURNS BIGINT STABLE AS $$
            SELECT (extract(epoch from now()) * 1000)::BIGINT
        $$ LANGUAGE sql;
        SELECT create_hypertable('trades', 'time', chunk_time_interval => 604800000) WHERE NOT EXISTS (SELECT TRUE FROM timescaledb_information.hypertables WHERE hypertable_name = 'trades');
        SELECT set_integer_now_func('trades', 'system_clock') WHERE NOT EXISTS (SELECT TRUE FROM timescaledb_information.hypertables WHERE hypertable_name = 'trades');
        ALTER TABLE trades SET
        (
            timescaledb.compress,
            timescaledb.compress_segmentby = 'pair_id',
            timescaledb.compress_orderby = 'time DESC'
        );
        SELECT add_compression_policy('trades', 604800000) WHERE NOT EXISTS (SELECT TRUE FROM timescaledb_information.hypertables WHERE hypertable_name = 'trades');
        
        CREATE MATERIALIZED VIEW IF NOT EXISTS pairs_view AS (
            WITH timings AS (
                SELECT
                    (SELECT EXTRACT(EPOCH FROM CURRENT_DATE::TIMESTAMP)::BIGINT * 1000) AS min_time,
                    (SELECT EXTRACT(EPOCH FROM CURRENT_DATE::TIMESTAMP + INTERVAL '1 day')::BIGINT * 1000) AS max_time
            )
            SELECT
                pairs.id,
                COALESCE((SELECT SUM(price * quantity) FROM trades WHERE pair_id = pairs.id AND time BETWEEN timings.min_time AND timings.max_time AND maker_order_id IS NOT NULL), 0.0) AS order_volume,
                COALESCE((SELECT SUM(price * quantity) FROM trades WHERE pair_id = pairs.id AND time BETWEEN timings.min_time AND timings.max_time AND maker_pool_id IS NOT NULL), 0.0) AS pool_volume,
                (SELECT ARRAY[price, time] FROM trades WHERE pair_id = pairs.id AND time BETWEEN timings.min_time AND timings.max_time ORDER BY time ASC LIMIT 1) AS open_price_and_time,
                (SELECT ARRAY[MIN(price), MAX(price)] FROM trades WHERE pair_id = pairs.id AND time BETWEEN timings.min_time AND timings.max_time LIMIT 1) AS low_price_and_high_price,
                (SELECT ARRAY[price, time] FROM trades WHERE pair_id = ppair.id AND time BETWEEN timings.min_time AND timings.max_time ORDER BY time ASC LIMIT 1) AS psynthetic_open_price_and_time,
                (SELECT ARRAY[price, time] FROM trades WHERE pair_id = spair.id AND time BETWEEN timings.min_time AND timings.max_time ORDER BY time ASC LIMIT 1) AS ssynthetic_open_price_and_time,
                (SELECT ARRAY[MIN(price), MAX(price)] FROM trades WHERE pair_id = ppair.id AND time BETWEEN timings.min_time AND timings.max_time LIMIT 1) AS psynthetic_low_price_and_high_price,
                (SELECT ARRAY[MIN(price), MAX(price)] FROM trades WHERE pair_id = spair.id AND time BETWEEN timings.min_time AND timings.max_time LIMIT 1) AS ssynthetic_low_price_and_high_price
            FROM pairs
                LEFT JOIN pairs ppair ON ppair.primary_asset_id = pairs.primary_asset_id AND ppair.secondary_asset_id IS NULL
                LEFT JOIN pairs spair ON spair.primary_asset_id = pairs.secondary_asset_id AND spair.secondary_asset_id IS NULL
                INNER JOIN assets passet ON passet.id = pairs.primary_asset_id
                INNER JOIN assets sasset ON sasset.id = pairs.secondary_asset_id
                INNER JOIN timings ON TRUE
        );
        CREATE MATERIALIZED VIEW IF NOT EXISTS pools_view AS (
            WITH timings AS (
                SELECT
                    (SELECT EXTRACT(EPOCH FROM CURRENT_DATE::TIMESTAMP)::BIGINT * 1000) AS min_time,
                    (SELECT EXTRACT(EPOCH FROM CURRENT_DATE::TIMESTAMP + INTERVAL '1 day')::BIGINT * 1000) AS max_time
            )
            SELECT
                pools.id,
                COALESCE((SELECT SUM(price * quantity) FROM trades WHERE pair_id = pools.pair_id AND time BETWEEN timings.min_time AND timings.max_time AND maker_pool_id = pools.id), 0.0) AS volume
            FROM pools
                INNER JOIN timings ON TRUE
        );
        CREATE MATERIALIZED VIEW IF NOT EXISTS delegators_view AS (
            WITH sources AS (
                SELECT
                    delegators.id,
                    delegators.account_id,
                    delegated_pools.pair_id
                FROM delegated_pools
                    INNER JOIN delegators ON delegators.id = delegated_pools.delegator_id
                GROUP BY delegators.id, delegators.account_id, delegated_pools.pair_id
            ), timings AS (
                SELECT
                    (SELECT EXTRACT(EPOCH FROM CURRENT_DATE::TIMESTAMP)::BIGINT * 1000) AS min_time,
                    (SELECT EXTRACT(EPOCH FROM CURRENT_DATE::TIMESTAMP + INTERVAL '1 day')::BIGINT * 1000) AS max_time
            )
            SELECT
                sources.id,
                sources.pair_id,
                (SELECT COALESCE(SUM(trades.price * trades.quantity), 0) FROM trades
                    INNER JOIN pools ON pools.id = maker_pool_id AND pools.account_id = sources.account_id
                WHERE trades.pair_id = sources.pair_id AND time BETWEEN timings.min_time AND timings.max_time) AS volume
            FROM sources
                INNER JOIN timings ON TRUE
        )`.simple());
    }
    static async isolate<T>(callback: (sql: pq.TransactionSql) => T | Promise<T>) {
        return await this.connection.begin(callback);
    }
    static async listen(channel: Notification, callback: (notification: { query: Record<string, any>, args: Record<string, any> }) => any, connection?: pq.TransactionSql): Promise<void> {
        const sql = connection || this.connection;
        await this.unlisten(channel);
        this.listeners[channel] = await sql.listen(channel, (message: string) => {
            try {
                Log.query(`exchange ${channel} notification: ${message}`);
                callback(JSON.parse(message));
            } catch (e) {
                Log.error(`exchange ${channel} notification error:`, e, `(message: ${message})`);
            }
        });
        Log.info(`exchange ${channel} channel: now listening`);
    }
    static async notify(channel: Notification, notification: { query: Record<string, any>, args: Record<string, any> }, connection?: pq.TransactionSql): Promise<boolean> {
        const sql = connection || this.connection;
        try {
            await sql.notify(channel, JSON.stringify(notification));
            return true;
        } catch {
            return false;
        }
    }
    static async unlisten(channel: Notification): Promise<void> {
        const listener = this.listeners[channel];
        if (!listener)
            return;

        delete this.listeners[channel];
        await listener.unlisten();
        Log.info(`exchange ${channel} channel: shutdown`);
    }
    static async dispatchEvents(block: { number: number, time: Date }, contract: { account: string, version: string, type: string }, events: EventInfo[], connection?: pq.TransactionSql): Promise<void> {
        const accounts: Record<string, Uint256> = { };
        if (!accounts[contract.account]) {
            const accountId = await this.getAccountIdByAddress(contract.account, false, connection);
            if (!accountId)
                throw new Error('cannot decode contract account ' + contract.account);

            accounts[contract.account] = accountId;
            if (MARKET_CLEANUP) {
                switch (contract.type) {
                    case 'dex': {
                        const market = await this.getMarketByAccountId(accountId, connection);
                        if (market != null) {
                            await this.cleanupLogs(market.id, block.number, connection);
                        }
                        break;
                    }
                }
            }
        }
        
        let step = 0;
        const pseudos: Record<string, any> = { };
        const orders: Record<string, { orderId: Uint256, pseudoRef: PseudoOrder | null, primaryQuantity: BigNumber, secondaryQuantity: BigNumber }> = { };
        const pools: Record<string, { poolId: Uint256, pseudoRef: PseudoPool | null, primaryQuantity: BigNumber, secondaryQuantity: BigNumber }> = { };
        const trades: { makerOrderOrPoolId: Uint256, makerOrderId: Uint256 | null, makerPoolId: Uint256 | null, takerOrderId: Uint256, side: OrderSide, price: BigNumber, quantity: BigNumber }[] = [];
        for (let i = 0; i < events.length; i++) {
            const log = events[i];
            Log.query(`exchange ${contract.account} event (type: ${log.event.type}, block: ${block.number}):`, log.event.args);
            switch (contract.type) {
                case 'dex': {
                    switch (log.event.type) {
                        case Spot.DEX.Events.Config: {
                            try {
                                const market = await Blockchain.call(contract.account, Readability.toFunction(Spot.DEX.paramsOf), []);
                                if (market != null) {
                                    if (!accounts[market.deployer_account]) {
                                        const accountId = await this.getAccountIdByAddress(market.deployer_account, false, connection);
                                        if (!accountId)
                                            throw new Error('cannot decode deployer account ' + market.deployer_account);
                                        accounts[market.deployer_account] = accountId;
                                    }
                                    
                                    const result = await this.setMarket({
                                        accountId: accounts[contract.account],
                                        deployerAccountId: accounts[market.deployer_account],
                                        blockNumber: block.number,
                                        poolExitFee: Common.bn(market.pool_exit_fee) || new BigNumber(0),
                                        maxPoolFeeRate: Common.bn(market.max_pool_fee_rate) || new BigNumber(0),
                                        minMakerFee: Common.bn(market.min_maker_fee) || new BigNumber(0),
                                        maxMakerFee: Common.bn(market.max_maker_fee) || new BigNumber(0),
                                        makerFeeExponent: Common.num(market.maker_fee_exponent) || 0,
                                        minTakerFee: Common.bn(market.min_taker_fee) || new BigNumber(0),
                                        maxTakerFee: Common.bn(market.max_taker_fee) || new BigNumber(0),
                                        takerFeeExponent: Common.num(market.taker_fee_exponent) || 0,
                                        assetVolumeTarget: Common.bn(market.asset_volume_target) || new BigNumber(0),
                                        assetResetDays: Common.num(market.asset_reset_days) || 0,
                                        accountResetDays: Common.num(market.account_reset_days) || 0,
                                        marketPolicy: (Common.num(market.market_policy) || 0) as MarketPolicy
                                    }, connection);
                                    if (result != null) {
                                        await this.notify(Notification.MarketUpdate, {
                                            query: { },
                                            args: { marketId: result.id }
                                        }, connection);
                                    }
                                }
                                Log.info(`exchange ${contract.account} market update`);
                            } catch (exception) {
                                Log.error(`exchange ${contract.account} market update error:`, exception);
                            }
                            break;
                        }
                        case Spot.DEX.Events.Order: {
                            let pseudo: PseudoOrder | null = null;
                            if (!pseudos[log.hash]) {
                                try {
                                    const orderParameters = () => ({
                                        transaction: log,
                                        primaryAsset: new AssetId(log.args[0]),
                                        secondaryAsset: new AssetId(log.args[1]),
                                        side: parseInt(log.args[2].toString()) as OrderSide,
                                        policy: parseInt(log.args[3].toString()) as OrderPolicy,
                                        value: log.pays.reduce((x, y) => x.plus(y.value), new BigNumber(0))
                                    });
                                    switch (log.method ? Readability.toFunction(log.method) : null) {
                                        case Readability.toFunction(Spot.DEX.marketOrder):
                                            pseudo = {
                                                ...orderParameters(),
                                                condition: OrderCondition.Market,
                                                slippage: Common.bn(log.args[4]),
                                            };
                                            break;
                                        case Readability.toFunction(Spot.DEX.limitOrder):
                                            pseudo = {
                                                ...orderParameters(),
                                                condition: OrderCondition.Limit,
                                                price: Common.bn(log.args[4]),
                                            };
                                            break;
                                        case Readability.toFunction(Spot.DEX.stopOrder):
                                            pseudo = {
                                                ...orderParameters(),
                                                condition: OrderCondition.Stop,
                                                stopPrice: Common.bn(log.args[4]),
                                                slippage: Common.bn(log.args[5])
                                            };
                                            break;
                                        case Readability.toFunction(Spot.DEX.stopLimitOrder):
                                            pseudo = {
                                                ...orderParameters(),
                                                condition: OrderCondition.StopLimit,
                                                stopPrice: Common.bn(log.args[4]),
                                                price: Common.bn(log.args[5])
                                            };
                                            break;
                                        case Readability.toFunction(Spot.DEX.trailingStopOrder):
                                            pseudo = {
                                                ...orderParameters(),
                                                condition: OrderCondition.TrailingStop,
                                                stopPrice: Common.bn(log.args[4]),
                                                slippage: Common.bn(log.args[5]),
                                                trailingStep: Common.bn(log.args[6]),
                                                trailingDistance: Common.bn(log.args[7]),
                                            };
                                            break;
                                        case Readability.toFunction(Spot.DEX.trailingStopLimitOrder):
                                            pseudo = {
                                                ...orderParameters(),
                                                condition: OrderCondition.TrailingStopLimit,
                                                stopPrice: Common.bn(log.args[4]),
                                                price: Common.bn(log.args[5]),
                                                trailingStep: Common.bn(log.args[6]),
                                                trailingDistance: Common.bn(log.args[7])
                                            };
                                            break;
                                    }
                                } catch { }
                                pseudos[log.hash] = { order: pseudo };
                            }

                            const orderId = new Uint256(log.event.args[0].toString());
                            orders[orderId.toString()] = {
                                orderId: orderId,
                                pseudoRef: pseudo,
                                primaryQuantity: new BigNumber(0),
                                secondaryQuantity: new BigNumber(0)
                            };
                            break;
                        }
                        case Spot.DEX.Events.Pool: {
                            let pseudo: PseudoPool | null = null;
                            if (!pseudos[log.hash]) {
                                try {
                                    switch (log.method ? Readability.toFunction(log.method) : null) {
                                        case Readability.toFunction(Spot.DEX.depositPool):
                                            pseudo = {
                                                transaction: log,
                                                primaryAsset: new AssetId(log.args[0]),
                                                secondaryAsset: new AssetId(log.args[1]),
                                                price: Common.bn(log.args[2]) || new BigNumber(0),
                                                minPrice: Common.bn(log.args[3]),
                                                maxPrice: Common.bn(log.args[4]),
                                                feeRate: Common.bn(log.args[5]) || new BigNumber(0),
                                            };
                                            break;
                                    }
                                } catch { }
                                pseudos[log.hash] = { pool: pseudo };
                            }

                            const poolId = new Uint256(log.event.args[0].toString());
                            pools[poolId.toString()] = {
                                poolId: poolId,
                                pseudoRef: pseudo,
                                primaryQuantity: new BigNumber(0),
                                secondaryQuantity: new BigNumber(0)
                            };
                            break;
                        }
                        case Spot.DEX.Events.Swap: {
                            const refType = parseInt(log.event.args[2]) as RefType;
                            const makerOrderOrPoolId = new Uint256(log.event.args[0].toString());
                            const makerOrderId = refType == RefType.Order ? makerOrderOrPoolId : null;
                            const makerPoolId = refType == RefType.Pool ? makerOrderOrPoolId : null;
                            const takerOrderId = new Uint256(log.event.args[1].toString());
                            const virtualTakerOrderRefId = takerOrderId.gt(0) ? takerOrderId : new Uint256(log.hash);
                            const side = parseInt(log.event.args[3]) as OrderSide;
                            const price = new BigNumber(log.event.args[4].toString());
                            const quantity = new BigNumber(log.event.args[5].toString());
                            if (makerOrderId != null) {
                                orders[makerOrderId.toString()] = {
                                    orderId: makerOrderId,
                                    pseudoRef: null,
                                    primaryQuantity: (orders[makerOrderId.toString()]?.primaryQuantity || new BigNumber(0)).plus(quantity),
                                    secondaryQuantity: (orders[makerOrderId.toString()]?.secondaryQuantity || new BigNumber(0)).plus(price.multipliedBy(quantity))
                                };
                            }
                            if (makerPoolId != null) {
                                pools[makerPoolId.toString()] = {
                                    poolId: makerPoolId,
                                    pseudoRef: null,
                                    primaryQuantity: (pools[makerPoolId.toString()]?.primaryQuantity || new BigNumber(0)).plus(quantity),
                                    secondaryQuantity: (pools[makerPoolId.toString()]?.secondaryQuantity || new BigNumber(0)).plus(price.multipliedBy(quantity))
                                };
                            }
                            orders[virtualTakerOrderRefId.toString()] = {
                                orderId: virtualTakerOrderRefId,
                                pseudoRef: takerOrderId.gt(0) ? orders[virtualTakerOrderRefId.toString()]?.pseudoRef || null : pseudos[log.hash] || null,
                                primaryQuantity: (orders[virtualTakerOrderRefId.toString()]?.primaryQuantity || new BigNumber(0)).plus(quantity),
                                secondaryQuantity: (orders[virtualTakerOrderRefId.toString()]?.secondaryQuantity || new BigNumber(0)).plus(price.multipliedBy(quantity)),
                            };
                            trades.push({
                                makerOrderOrPoolId: makerOrderOrPoolId,
                                makerOrderId: makerOrderId,
                                makerPoolId: makerPoolId,
                                takerOrderId: virtualTakerOrderRefId,
                                side: side,
                                price: price,
                                quantity: quantity
                            });
                            break;
                        }
                        case Spot.DEX.Events.AssetTier: {
                            const asset = new AssetId(log.event.args[0].toString());
                            try {
                                const accountId = await this.getAccountIdByAddress(contract.account, false, connection);
                                if (!accountId)
                                    throw new Error('Failed to get contract account id: ' + contract.account);

                                const market = await this.getMarketByAccountId(accountId, connection);
                                if (!market)
                                    throw new Error('Failed to get market: ' + contract.account);

                                const tier = await Blockchain.call(contract.account, Readability.toFunction(Spot.DEX.assetOf), [asset.toUint256()]);
                                const assetId = await this.getAssetIdByHash(asset, 'trusted', connection);
                                if (!assetId)
                                    throw new Error('Failed to get asset id');

                                await this.setPolyAsset(assetId, market, tier ? tier.symbol || null : null, connection);
                                Log.info(`exchange ${contract.account} poly asset update: ${asset.id} maps to ${tier ? tier.symbol || null : null}`);
                            } catch (exception) {
                                Log.error(`exchange ${contract.account} poly asset update error (ref: ${asset.id}):`, exception);
                            }
                            break;
                        }
                        default:
                            Log.error(`exchange ${contract.account} event is unknown (type: ${log.event.type}, block: ${block.number}, contract: dex):`, log.event.args);
                            break;
                    }
                    break;
                }
                case 'dlp': {
                    switch (log.event.type) {
                        case Spot.DLP.Events.Config: {
                            try {
                                const delegator = await Blockchain.call(contract.account, Readability.toFunction(Spot.DLP.paramsOf), []);
                                if (delegator != null) {
                                    if (!accounts[delegator.deployer_account]) {
                                        const accountId = await this.getAccountIdByAddress(delegator.deployer_account, false, connection);
                                        if (!accountId)
                                            throw new Error('cannot decode deployer account ' + delegator.deployer_account);
                                        accounts[delegator.deployer_account] = accountId;
                                    }

                                    const marketAccountId = await this.getAccountIdByAddress(delegator.dex_account, true, connection);
                                    if (!marketAccountId)
                                        throw new Error('cannot find account ' + delegator.dex_account);

                                    const market = await this.getMarketByAccountId(marketAccountId, connection);
                                    if (!market)
                                        throw new Error('cannot find market account ' + delegator.dex_account);

                                    const permissions = Array.isArray(delegator.permissions) ? delegator.permissions.map((x: any) => ({
                                        primaryAssetId: null,
                                        primaryAsset: x.primary_asset,
                                        secondaryAssetId: null,
                                        secondaryAsset: x.secondary_asset
                                    })) : [];
                                    for (let i = 0; i < permissions.length; i++) {
                                        const permission = permissions[i];
                                        permission.primaryAssetId = await this.getAssetIdByHash(new AssetId(permission.primaryAsset), 'trusted', connection);
                                        permission.secondaryAssetId = await this.getAssetIdByHash(new AssetId(permission.secondaryAsset), 'trusted', connection);
                                    }
                                    
                                    const result = await this.setDelegator({
                                        marketId: market.id,
                                        accountId: accounts[contract.account],
                                        deployerAccountId: accounts[delegator.deployer_account],
                                        blockNumber: block.number,
                                        rewardEmission: Common.bn(delegator.reward_emission) || new BigNumber(0),
                                        rewardBalance: Common.bn(delegator.reward_balance) || new BigNumber(0),
                                        permissions: permissions.filter((x: any) => x.primaryAssetId != null && x.secondaryAssetId != null)
                                    }, connection);
                                    if (result != null) {
                                        await this.notify(Notification.DelegatorUpdate, {
                                            query: { },
                                            args: { delegatorId: result.id }
                                        }, connection);
                                    }
                                }
                                Log.info(`exchange ${contract.account} delegator update`);
                            } catch (exception) {
                                Log.error(`exchange ${contract.account} delegator update error:`, exception);
                            }
                            break;
                        }
                        case Spot.DLP.Events.PoolRefEvent: {
                            const primaryAsset = new AssetId(log.event.args[0]);
                            const secondaryAsset = new AssetId(log.event.args[1]);
                            const ownerPubkeyhash = new Pubkeyhash(log.event.args[2]);
                            const owner = Signing.encodeAddress(ownerPubkeyhash);
                            const batch = log.event.args[3];
                            if (batch) {
                                try {
                                    const accountId = await this.getAccountIdByAddress(contract.account, false, connection);
                                    if (!accountId)
                                        throw new Error('Failed to get delegator account id: ' + contract.account);

                                    const delegator = await this.getDelegatorByAccountId(accountId, connection);
                                    if (!delegator)
                                        throw new Error('Failed to get delegator: ' + contract.account);

                                    const primaryAssetId = await this.getAssetIdByHash(primaryAsset, 'trusted', connection);
                                    if (!primaryAssetId)
                                        throw new Error('cannot find primary asset of contract account ' + contract.account);

                                    const secondaryAssetId = await this.getAssetIdByHash(secondaryAsset, 'trusted', connection);
                                    if (!secondaryAssetId)
                                        throw new Error('cannot find secondary asset of contract account ' + contract.account);

                                    const market = await this.getMarketById(delegator.marketId, connection);
                                    if (!market)
                                        throw new Error('cannot find market of delegator contract account ' + contract.account);

                                    const pairId = await this.getPairByAssetIds(primaryAssetId, secondaryAssetId, market.id, true, connection);
                                    if (!pairId)
                                        throw new Error('cannot find asset pair of contract account ' + contract.account);

                                    const delegatedPools = await this.getAllDelegatedPoolsByDelegatorIdAndMarketPair(delegator.id, delegator.marketId, pairId)
                                    for (let i = 0; i < delegatedPools.length; i++) {
                                        const delegatedPool = delegatedPools[i];
                                        let delegatedPoolId: Uint256 | null = null;
                                        let owner: string | null = null;
                                        try {
                                            const account = await this.getAccountHashById(delegatedPool.accountId, connection);
                                            if (!account)
                                                throw new Error('Failed to get delegated pool account id: ' + delegatedPool.accountId);

                                            owner = Signing.encodeAddress(account);
                                            const share = await Blockchain.call(contract.account, Readability.toFunction(Spot.DLP.shareOf), [primaryAsset.toUint256(), secondaryAsset.toUint256(), owner]);
                                            const result = await this.setDelegatedPool({
                                                pairId: pairId,
                                                marketId: market.id,
                                                delegatorId: delegator.id,
                                                accountId: delegatedPool.accountId,
                                                blockNumber: block.number,
                                                rewardValue: Common.bn(share.reward_value) || new BigNumber(0),
                                                initialPrimaryValue: Common.bn(share.primary_value) || new BigNumber(0),
                                                initialSecondaryValue: Common.bn(share.secondary_value) || new BigNumber(0),
                                                primaryValue: Common.bn(share.primary_value) || new BigNumber(0),
                                                secondaryValue: Common.bn(share.secondary_value) || new BigNumber(0),
                                                active: true
                                            }, false);
                                            if (result) {
                                                delegatedPoolId = result.id;
                                            }
                                        } catch {
                                            const prevDelegatedPool = await this.getDelegatedPoolByHandle(market.id, pairId, delegator.id, delegatedPool.accountId, connection);
                                            if (prevDelegatedPool) {
                                                delegatedPoolId = prevDelegatedPool.id;
                                                prevDelegatedPool.blockNumber = block.number;
                                                prevDelegatedPool.active = false;
                                                await this.setDelegatedPool(prevDelegatedPool, false, connection);
                                            }
                                        }

                                        if (owner && delegatedPoolId) {
                                            await this.notify(Notification.DelegatedPoolUpdate, {
                                                query: { accounts: [owner] },
                                                args: { delegatedPoolId: delegatedPoolId }
                                            }, connection);
                                        }
                                    }

                                    Log.info(`exchange ${contract.account} delegated pool batch update (market_id: ${market.id.toString()}, pair_id: ${pairId.toString()}, delegator_id: ${delegator.id.toString()}): ${delegatedPools.length} updates`);
                                } catch (exception) {
                                    Log.error(`exchange ${contract.account} delegated pool batch update error (primary_asset: ${primaryAsset.id}, secondary_asset: ${secondaryAsset.id}):`, exception);
                                }
                            }

                            if (!ownerPubkeyhash.equals(new Pubkeyhash())) {
                                try {
                                    const ownerId = owner ? await this.getAccountIdByAddress(owner, false, connection) : null;
                                    if (!ownerId)
                                        throw new Error('Failed to get owner account id: ' + owner);

                                    const accountId = await this.getAccountIdByAddress(contract.account, false, connection);
                                    if (!accountId)
                                        throw new Error('Failed to get delegator account id: ' + contract.account);

                                    const delegator = await this.getDelegatorByAccountId(accountId, connection);
                                    if (!delegator)
                                        throw new Error('Failed to get delegator: ' + contract.account);

                                    const primaryAssetId = await this.getAssetIdByHash(primaryAsset, 'trusted', connection);
                                    if (!primaryAssetId)
                                        throw new Error('cannot find primary asset of contract account ' + contract.account);

                                    const secondaryAssetId = await this.getAssetIdByHash(secondaryAsset, 'trusted', connection);
                                    if (!secondaryAssetId)
                                        throw new Error('cannot find secondary asset of contract account ' + contract.account);

                                    const market = await this.getMarketById(delegator.marketId, connection);
                                    if (!market)
                                        throw new Error('cannot find market of delegator contract account ' + contract.account);

                                    const pairId = await this.getPairByAssetIds(primaryAssetId, secondaryAssetId, market.id, true, connection);
                                    if (!pairId)
                                        throw new Error('cannot find asset pair of contract account ' + contract.account);

                                    let delegatedPoolId: Uint256 | null = null;
                                    try {
                                        const share = await Blockchain.call(contract.account, Readability.toFunction(Spot.DLP.shareOf), [primaryAsset.toUint256(), secondaryAsset.toUint256(), owner]);
                                        const result = await this.setDelegatedPool({
                                            pairId: pairId,
                                            marketId: market.id,
                                            delegatorId: delegator.id,
                                            accountId: ownerId,
                                            blockNumber: block.number,
                                            rewardValue: Common.bn(share.reward_value) || new BigNumber(0),
                                            initialPrimaryValue: Common.bn(share.primary_value) || new BigNumber(0),
                                            initialSecondaryValue: Common.bn(share.secondary_value) || new BigNumber(0),
                                            primaryValue: Common.bn(share.primary_value) || new BigNumber(0),
                                            secondaryValue: Common.bn(share.secondary_value) || new BigNumber(0),
                                            active: true
                                        }, true);
                                        if (result) {
                                            delegatedPoolId = result.id;
                                        }
                                    } catch {
                                        const prevDelegatedPool = await this.getDelegatedPoolByHandle(market.id, pairId, delegator.id, ownerId, connection);
                                        if (prevDelegatedPool) {
                                            delegatedPoolId = prevDelegatedPool.id;
                                            prevDelegatedPool.blockNumber = block.number;
                                            prevDelegatedPool.active = false;
                                            await this.setDelegatedPool(prevDelegatedPool, false, connection);
                                        }
                                    }             

                                    await this.notify(Notification.DelegatedPoolUpdate, {
                                        query: { accounts: [owner] },
                                        args: { delegatedPoolId: delegatedPoolId || new Uint256(0) }
                                    }, connection);
                                    Log.info(`exchange ${contract.account} delegated pool update (market_id: ${market.id.toString()}, pair_id: ${pairId.toString()}, delegator_id: ${delegator.id.toString()}, ownerId: ${ownerId.toString()})`);
                                } catch (exception) {
                                    Log.error(`exchange ${contract.account} delegated pool update error (primary_asset: ${primaryAsset.id}, secondary_asset: ${secondaryAsset.id}, owner: ${owner}):`, exception);
                                }
                            }
                            break;
                        }
                        default:
                            Log.error(`exchange ${contract.account} event is unknown (type: ${log.event.type}, block: ${block.number}, contract: dlp):`, log.event.args);
                            break;
                    }
                    break;
                }
                default:
                    Log.error(`exchange ${contract.account} event is unknown (type: ${log.event.type}, block: ${block.number}):`, log.event.args);
                    break;
            }
        }

        for (let target in orders) {
            const event = orders[target];
            try {
                let result: Order | null = null, order: any;
                try {
                    const pseudoOrder: PseudoOrder = event.pseudoRef as any;
                    try {
                        order = await Blockchain.call(contract.account, Readability.toFunction(Spot.DEX.orderOf), [event.orderId]);
                    } catch (exception) {
                        if (!event.pseudoRef)
                            throw exception;
                    }

                    if (!order && !event.pseudoRef)
                        throw new Error('cannot find order of contract account ' + contract.account);

                    const account = order ? order.account : pseudoOrder.transaction.fromAccount; 
                    if (!accounts[account]) {
                        const accountId = await this.getAccountIdByAddress(account, false, connection);
                        if (!accountId)
                            throw new Error('cannot decode order account ' + account);
                        accounts[account] = accountId;
                    }
                    
                    const pair = order ? await Blockchain.call(contract.account, Readability.toFunction(Spot.DEX.pairOf), [order.pair_id]) : { primary_asset: pseudoOrder.primaryAsset.toHex(), secondary_asset: pseudoOrder.secondaryAsset.toHex() };
                    if (!pair)
                        throw new Error('cannot find asset pair of contract account ' + contract.account);

                    const primaryAssetId = await this.getAssetIdByHash(new AssetId(pair.primary_asset), 'trusted', connection);
                    if (!primaryAssetId)
                        throw new Error('cannot find primary asset of contract account ' + contract.account);

                    const secondaryAssetId = await this.getAssetIdByHash(new AssetId(pair.secondary_asset), 'trusted', connection);
                    if (!secondaryAssetId)
                        throw new Error('cannot find secondary asset of contract account ' + contract.account);

                    const market = await this.getMarketByAccountId(accounts[contract.account], connection);
                    if (!market)
                        throw new Error('cannot find market of contract account ' + contract.account);

                    const pairId = await this.getPairByAssetIds(primaryAssetId, secondaryAssetId, market.id, true, connection);
                    if (!pairId)
                        throw new Error('cannot find asset pair of contract account ' + contract.account);

                    const startingValue = BigNumber.max(pseudoOrder ? pseudoOrder.value : 0, order ? order.value : 0);
                    const currentValue = new BigNumber(order ? order.value : BigNumber.max(0, pseudoOrder.value.minus(pseudoOrder.side == OrderSide.Buy ? event.secondaryQuantity : event.primaryQuantity)));
                    result = await this.setOrder({
                        orderId: order ? Common.u256(order.id) || new Uint256(order.id) : new Uint256(pseudoOrder.transaction.hash),
                        pairId: pairId,
                        marketId: market.id,
                        accountId: accounts[account],
                        blockNumber: block.number,
                        condition: (Common.num(order ? order.condition : pseudoOrder.condition) || 0) as OrderCondition,
                        side: (Common.num(order ? order.side : pseudoOrder.side) || 0) as OrderSide,
                        policy: (Common.num(order ? order.policy : pseudoOrder.policy) || 0) as OrderPolicy,
                        price: Common.bn(order ? order.price : pseudoOrder.price),
                        stopPrice: Common.bn(order ? order.stop_price : pseudoOrder.stopPrice),
                        fillingPrice: event.primaryQuantity.gt(0) ? Common.bn(event.secondaryQuantity.dividedBy(event.primaryQuantity)) : undefined,
                        startingValue: BigNumber.max(startingValue, currentValue),
                        value: currentValue,
                        slippage: Common.bn(order ? order.slippage : pseudoOrder.slippage),
                        trailingStep: Common.bn(order ? order.trailing_step : pseudoOrder.trailingStep),
                        trailingDistance: Common.bn(order ? order.trailing_distance : pseudoOrder.trailingDistance),
                        active: order != null && currentValue.isGreaterThan(0) ? true : false,
                        lastPrice: new BigNumber(0),
                        lastQuantity: new BigNumber(0),
                    }, connection);
                } catch {
                    const prevOrder = await this.getOrderByOrderId(event.orderId, connection);
                    if (prevOrder) {
                        prevOrder.fillingPrice = event.primaryQuantity.gt(0) ? Common.bn(event.secondaryQuantity.dividedBy(event.primaryQuantity)) : undefined;
                        prevOrder.value = BigNumber.max(0, prevOrder.value.minus(prevOrder.side == OrderSide.Buy ? event.secondaryQuantity : event.primaryQuantity));
                        prevOrder.blockNumber = block.number;
                        prevOrder.active = false;
                        result = await this.setOrder(prevOrder, connection);
                    }
                }

                if (result != null) {
                    const account = Signing.encodeAddress(await this.getAccountHashById(result.accountId, connection) || new Pubkeyhash());
                    if (!account)
                        throw new Error('cannot decode order account ' + result.accountId);

                    await Promise.all([
                        this.notify(Notification.OrderUpdate, {
                            query: { accounts: [account] },
                            args: { orderId: result.id }
                        }, connection),
                        this.notify(Notification.LevelUpdate, {
                            query: { },
                            args: result.active && result.lastPrice.gt(0) && result.lastQuantity.gt(0) ? {
                                id: result.id,
                                side: result.side,
                                price: result.lastPrice,
                                quantity: result.lastQuantity
                            } as AggregatedLevel : { id: result.id }
                        }, connection)
                    ]);
                }
                Log.info(`exchange ${contract.account} order update (order_id: ${event.orderId.toString()})`);
            } catch (exception) {
                Log.error(`exchange ${contract.account} order update error (order_id: ${event.orderId.toString()}):`, exception);
            }
        }

        for (let target in pools) {
            const event = pools[target];
            try {
                let result: Pool | null = null, pool: any;
                try {
                    const pseudoPool: PseudoPool = event.pseudoRef as any;
                    try {
                        pool = await Blockchain.call(contract.account, Readability.toFunction(Spot.DEX.poolOf), [event.poolId]);
                    } catch (exception) {
                        if (!event.pseudoRef)
                            throw exception;
                    }

                    if (!pool && !event.pseudoRef)
                        throw new Error('cannot find pool of contract account ' + contract.account);

                    const account = pool ? pool.account : pseudoPool.transaction.fromAccount; 
                    if (!accounts[account]) {
                        const accountId = await this.getAccountIdByAddress(account, false, connection);
                        if (!accountId)
                            throw new Error('cannot decode pool account ' + account);
                        accounts[account] = accountId;
                    }
                    
                    const pair = pool ? await Blockchain.call(contract.account, Readability.toFunction(Spot.DEX.pairOf), [pool.pair_id]) : { primary_asset: pseudoPool.primaryAsset.toHex(), secondary_asset: pseudoPool.secondaryAsset.toHex() };
                    if (!pair)
                        throw new Error('cannot find asset pair of contract account ' + contract.account);

                    const primaryAsset = new AssetId(pair.primary_asset);
                    const primaryAssetId = await this.getAssetIdByHash(primaryAsset, 'trusted', connection);
                    if (!primaryAssetId)
                        throw new Error('cannot find primary asset of contract account ' + contract.account);

                    const secondaryAsset = new AssetId(pair.secondary_asset);
                    const secondaryAssetId = await this.getAssetIdByHash(secondaryAsset, 'trusted', connection);
                    if (!secondaryAssetId)
                        throw new Error('cannot find secondary asset of contract account ' + contract.account);

                    const market = await this.getMarketByAccountId(accounts[contract.account], connection);
                    if (!market)
                        throw new Error('cannot find market of contract account ' + contract.account);

                    const pairId = await this.getPairByAssetIds(primaryAssetId, secondaryAssetId, market.id, true, connection);
                    if (!pairId)
                        throw new Error('cannot find asset pair of contract account ' + contract.account);

                    const primaryValue = pool ? new BigNumber(pool.primary_value) : pseudoPool.transaction.pays.filter(x => x.asset.token == primaryAsset.token).reduce((x, y) => x.plus(y.value), new BigNumber(0))
                    const secondaryValue = pool ? new BigNumber(pool.secondary_value) : pseudoPool.transaction.pays.filter(x => x.asset.token == secondaryAsset.token).reduce((x, y) => x.plus(y.value), new BigNumber(0))
                    const primaryRevenue = pool ? new BigNumber(pool.primary_revenue) : new BigNumber(0);
                    const secondaryRevenue = pool ? new BigNumber(pool.secondary_revenue) : new BigNumber(0);
                    const minPrice = pool ? Common.bn(pool.min_price)?.pow(2) : pseudoPool.minPrice;
                    const maxPrice = pool ? Common.bn(pool.max_price)?.pow(2) : pseudoPool.maxPrice;
                    const concentrated = minPrice?.gt(0) && maxPrice?.gt(0);
                    const price = pool ? new BigNumber(pool.price).pow(concentrated ? 2 : 1) : pseudoPool.price;
                    result = await this.setPool({
                        poolId: pool ? Common.u256(pool.id) || new Uint256(pool.id) : event.poolId,
                        pairId: pairId,
                        marketId: market.id,
                        accountId: accounts[account],
                        blockNumber: block.number,
                        initialPrice: price,
                        initialPrimaryValue: primaryValue.plus(primaryRevenue),
                        initialSecondaryValue: secondaryValue.plus(secondaryRevenue),
                        primaryValue: primaryValue,
                        secondaryValue: secondaryValue,
                        primaryRevenue: primaryRevenue,
                        secondaryRevenue: secondaryRevenue,
                        liquidity: pool ? new BigNumber(pool.liquidity) : new BigNumber(0),
                        price: price,
                        minPrice: minPrice,
                        maxPrice: maxPrice,
                        feeRate: pool ? new BigNumber(pool.fee_rate) : pseudoPool.feeRate,
                        exitFee: pool ? new BigNumber(pool.exit_fee) : new BigNumber(0),
                        lastAskPrice: new BigNumber(0),
                        lastBidPrice: new BigNumber(0),
                        active: pool != null
                    }, connection);
                    if (result != null && (pseudoPool != null || result.initialPrice.eq(result.price))) {
                        await this.setDepth({
                            poolId: result.id,
                            pairId: pairId,
                            marketId: market.id,
                            accountId: accounts[account],
                            blockNumber: block.number,
                            price: result.price,
                            quantity: result.primaryValue.plus(result.primaryRevenue).plus(result.secondaryValue.plus(result.secondaryRevenue).dividedBy(result.price)),
                            time: new Date(block.time.getTime() + 100 * step++)
                        }, connection);
                    }
                } catch {
                    const prevPool = await this.getPoolByPoolId(event.poolId, connection);
                    if (prevPool) {
                        prevPool.blockNumber = block.number;
                        prevPool.active = false;
                        result = await this.setPool(prevPool, connection);
                        if (result != null) {
                            await this.setDepth({
                                poolId: result.id,
                                pairId: result.pairId,
                                marketId: result.marketId,
                                accountId: result.accountId,
                                blockNumber: block.number,
                                price: result.price,
                                quantity: result.primaryValue.plus(result.primaryRevenue).plus(result.secondaryValue.plus(result.secondaryRevenue).dividedBy(result.price)).negated(),
                                time: new Date(block.time.getTime() + 100 * step++)
                            }, connection);
                        }
                    }
                }

                if (result != null) {
                    const account = Signing.encodeAddress(await this.getAccountHashById(result.accountId, connection) || new Pubkeyhash());
                    if (!account)
                        throw new Error('cannot decode order account ' + result.accountId);

                    const minPrice = result.minPrice || result.lastBidPrice.multipliedBy(0.9999);
                    const maxPrice = result.maxPrice || result.lastAskPrice.multipliedBy(1.0001);
                    const events = [this.notify(Notification.PoolUpdate, {
                        query: { accounts: [account] },
                        args: { poolId: result.id }
                    }, connection)];
                    if (result.active && ((result.lastAskPrice.gt(0) && result.lastAskPrice.lt(maxPrice) && result.primaryValue.gt(0)) || (result.lastBidPrice.gt(0) && result.lastBidPrice.gt(minPrice) && result.secondaryValue.gt(0)))) {
                        events.push(this.notify(Notification.LevelUpdate, {
                            query: { },
                            args: result.lastAskPrice.gt(0) && result.lastAskPrice.lt(maxPrice) && result.primaryValue.gt(0) ? {
                                id: result.id,
                                side: OrderSide.Sell,
                                price: result.lastAskPrice,
                                quantity: result.primaryValue,
                                curve: {
                                    minPrice: result.minPrice,
                                    maxPrice: result.maxPrice,
                                    primaryValue: result.primaryValue,
                                    secondaryValue: result.secondaryValue,
                                    feeRate: result.feeRate
                                }
                            } as AggregatedLevel : { id: result.id }
                        }, connection));
                        events.push(this.notify(Notification.LevelUpdate, {
                            query: { },
                            args: result.active && result.lastBidPrice.gt(0) && result.lastBidPrice.gt(minPrice) && result.secondaryValue.gt(0) ? {
                                id: result.id,
                                side: OrderSide.Buy,
                                price: result.lastBidPrice,
                                quantity: result.secondaryValue.dividedBy(result.lastBidPrice),
                                curve: {
                                    minPrice: result.minPrice,
                                    maxPrice: result.maxPrice,
                                    primaryValue: result.primaryValue,
                                    secondaryValue: result.secondaryValue,
                                    feeRate: result.feeRate
                                }
                            } as AggregatedLevel : { id: result.id }
                        }, connection));
                    } else {
                        events.push(this.notify(Notification.LevelUpdate, {
                            query: { },
                            args: { id: result.id }
                        }, connection));
                    }
                    await Promise.all(events);
                }
                Log.info(`exchange ${contract.account} pool update (pool_id: ${event.poolId.toString()})`);
            } catch (exception) {
                Log.error(`exchange ${contract.account} pool update error (pool_id: ${event.poolId.toString()}):`, exception);
            }
        }

        for (let i = 0; i < trades.length; i++) {
            const event = trades[i];
            try {
                const makerOrder = event.makerOrderId != null ? await this.getOrderByOrderId(event.makerOrderId, connection) : null;
                const makerPool = !makerOrder && event.makerPoolId != null ? await this.getPoolByPoolId(event.makerPoolId, connection) : null;
                const takerOrder = await this.getOrderByOrderId(event.takerOrderId, connection);
                if (!makerOrder && !makerPool && !takerOrder)
                    throw new Error('cannot find maker/taker order of contract account ' + contract.account + '(taker_order_id: ' + event.takerOrderId.toString() + ')');

                const marketId: Uint256 = (makerOrder?.marketId || makerPool?.marketId || takerOrder?.marketId) as any;
                const pairId: Uint256 = (makerOrder?.pairId || makerPool?.pairId || takerOrder?.pairId) as any;
                if (takerOrder && (marketId.neq(takerOrder.marketId) || pairId.neq(takerOrder.pairId)))
                    throw new Error('maker/taker order market mismatch of contract account ' + contract.account + '(maker_order_or_pool_id: ' + (event.makerOrderOrPoolId.toString() || 'null') + ', taker_order_id: ' + event.takerOrderId.toString() + ')');
            
                const assets = await this.getPairById(pairId, connection);
                if (!assets || !assets.primaryAsset || !assets.secondaryAsset)
                    throw new Error('cannot find assets of contract account ' + contract.account + '(order_id: ' + event.takerOrderId.toString() + ')');
                
                const base = assets && assets.secondaryAsset && Whitelist.has(assets.secondaryAsset.hash) ? Quotes.assetBaseOf(assets.secondaryAsset.hash) : null;
                const trade = {
                    pairId: pairId,
                    marketId: marketId,
                    makerOrderId: makerOrder?.id,
                    makerPoolId:  makerPool?.id,
                    makerAccountId: makerOrder?.accountId || makerPool?.accountId,
                    takerOrderId: takerOrder?.id,
                    takerAccountId: takerOrder?.accountId,
                    blockNumber: block.number,
                    side: event.side,
                    price: event.price,
                    quantity: event.quantity,
                    time: new Date(block.time.getTime() + 100 * (step + i))
                };
                const result = await this.setTrade(trade, connection);
                if (result != null) {
                    const accountId = takerOrder?.accountId || makerOrder?.accountId || makerPool?.accountId;
                    const account = accountId ? await this.getAccountHashById(accountId, connection) : null;
                    await this.notify(Notification.TradeUpdate, {
                        query: { },
                        args: {
                            primaryAsset: assets.primaryAsset.hash,
                            secondaryAsset: assets.secondaryAsset.hash,
                            secondaryBase: base,
                            account: account ? Signing.encodeAddress(account) : null,
                            side: result.side,
                            price: result.price,
                            quantity: result.quantity
                        }
                    }, connection);
                }
                    
                if (base != null) {
                    const pairId = await this.getPairByAssetIds(assets.primaryAsset.id, null, null,true,  connection);
                    if (pairId != null) {
                        trade.pairId = pairId;
                        await this.setTrade(trade, connection);
                    }
                }
                Log.info(`exchange ${contract.account} trade update: (maker_order_or_pool_id: ${event.makerOrderOrPoolId.toString() || 'null'}, taker_order_id: ${event.takerOrderId.toString()})`);
            } catch (exception) {
                Log.error(`exchange ${contract.account} trade update error:`, exception);
            }
        }
    }
    static async rollbackToBlock(blockNumber: number, connection?: pq.TransactionSql): Promise<void> {
        const sql = connection || this.connection;
        await this.resultOf(sql`DELETE FROM blocks WHERE block_number >= ${blockNumber}`);
        await this.resultOf(sql`UPDATE accounts SET synced = FALSE`);
    }
    static async setBlock(block: Block, affectedAccounts?: string[], connection?: pq.TransactionSql): Promise<Block | null> {
        const sql = connection || this.connection;
        const result = await this.resultOf(sql`
        INSERT INTO blocks
        (
            block_number,
            block_hash
        )
        VALUES
        (
            ${block.blockNumber},
            ${block.blockHash.toUint8Array()}
        )
        ON CONFLICT (block_number) DO UPDATE SET
            block_hash = EXCLUDED.block_hash
        RETURNING *`);
        
        if (affectedAccounts != null && affectedAccounts.length > 0) {
            let affectedAccountIds = [];
            for (let i = 0; i < affectedAccounts.length; i++) {
                const account = affectedAccounts[i];
                const id = await this.getAccountIdByAddress(account, true, connection);
                if (id != null) {
                    affectedAccountIds.push(id);
                }
            }
            if (affectedAccountIds.length > 0) {
                Log.info(`exchange account invalidation: (accounts: ${affectedAccountIds.length})`);
                await this.setAccountSyncByAccountIds(affectedAccountIds, false, connection);
            }
        }
        
        await this.notify(Notification.ChainUpdate, {
            query: { },
            args: { tip: block.blockNumber }
        }, connection);
        try {
            return this.toBlock(result[0]);
        } catch {
            return null;
        }
    }
    static async getLatestBlock(offset?: number, connection?: pq.TransactionSql): Promise<Block | null> {
        const sql = connection || this.connection;
        const result = await this.resultOf(sql`SELECT * FROM blocks ORDER BY block_number DESC LIMIT 1 OFFSET ${offset || 0}`);
        try {
            return this.toBlock(result[0]);
        } catch {
            return null;
        }
    }
    static async getBlockByHash(blockHash: Uint256, connection?: pq.TransactionSql): Promise<Block | null> {
        const sql = connection || this.connection;
        const result = await this.resultOf(sql`SELECT * FROM blocks WHERE block_hash = ${blockHash.toUint8Array()}`);
        try {
            return this.toBlock(result[0]);
        } catch {
            return null;
        }
    }
    static async getBlockByNumber(blockNumber: number, connection?: pq.TransactionSql): Promise<Block | null> {
        const sql = connection || this.connection;
        const result = await this.resultOf(sql`SELECT * FROM blocks WHERE block_number = ${blockNumber}`);
        try {
            return this.toBlock(result[0]);
        } catch {
            return null;
        }
    }
    static async setPolyAsset(assetId: Uint256, market: Market, symbol: string | null, connection?: pq.TransactionSql): Promise<AssetId | null> {
        const sql = connection || this.connection;
        if (!market.account)
            throw new Error('Failed to receive market by id');

        const polyAsset = symbol ? AssetId.fromHandle(new AssetId().chain || '', symbol, market.account) : null;
        await this.resultOf(sql`DELETE FROM poly_assets WHERE asset_id = ${assetId.toString()} AND market_id = ${market.id.toString()}`);
        if (!polyAsset)
            return null;

        const polyAssetId = await this.getAssetIdByHash(polyAsset, 'trusted', connection);
        if (!polyAssetId)
            return polyAsset;

        await this.resultOf(sql`INSERT INTO poly_assets (asset_id, market_id, poly_asset_id) VALUES (${assetId.toString()}, ${market.id.toString()}, ${polyAssetId.toString()}) ON CONFLICT (asset_id, market_id) DO NOTHING`);
        return polyAsset;
    }
    static async getPolyAssetId(assetId: Uint256, marketId: Uint256, connection?: pq.TransactionSql): Promise<Uint256 | null> {
        const sql = connection || this.connection;
        const key = assetId.toString() + ':' + marketId.toString();
        const cache = this.get(this.cache.assetIdToPolyAssetId, key);
        if (cache != null)
            return new Uint256(cache);

        let result = await this.resultOf(sql`SELECT poly_asset_id FROM poly_assets WHERE asset_id = ${assetId.toString()} AND market_id = ${marketId.toString()}`);
        try {
            const value = new Uint256(result[0]['poly_asset_id']);
            this.set(this.cache.assetIdToPolyAssetId, key, value.toString());
            return value;
        } catch {
            return null;
        }
    }
    static async getAssetIdByHash(hash: AssetId, mode: 'trusted' | 'untrusted' | 'read-only', connection?: pq.TransactionSql): Promise<Uint256 | null> {
        const sql = connection || this.connection;
        const cache = this.get(this.cache.assetHashToAssetId, hash.toHex());
        if (cache != null)
            return new Uint256(cache);

        let result = await this.resultOf(sql`SELECT id FROM assets WHERE hash = ${hash.toUint8Array()}`);
        if (!result.length) {
            if (!!hash.token != !!hash.checksum) {
                throw new Error('Impossible asset handle (token/contract mismatch)');
            } else if (mode == 'read-only') {
                return null;
            }

            const blockchains = await Blockchain.getBlockchains();
            const info = blockchains.find((v) => v.chain == hash.chain);
            if (info != null) {
                if (hash.token != null || hash.checksum != null) {
                    if (info.tokenPolicy == null || info.tokenPolicy == 'none')
                        throw new Error('Impossible asset handle (tokens are not supported)');

                    if (mode == 'untrusted') {
                        try {
                            const result = await Blockchain.getAssetHolders(hash);
                            if (!result || !new BigNumber(result).gt(0))
                                throw false;
                        } catch {
                            throw new Error('Asset must have some holders');
                        }
                    }
                }
            }
            else if (hash.chain != new AssetId().chain)
                throw new Error('Impossible asset handle (blockchain mismatch)');

            result = await this.resultOf(sql`INSERT INTO assets (hash) VALUES (${hash.toUint8Array()}) ON CONFLICT (hash) DO NOTHING RETURNING id`);
        }
        
        try {
            const value = new Uint256(result[0]['id']);
            this.set(this.cache.assetHashToAssetId, hash.toHex(), value.toString());
            return value;
        } catch {
            return null;
        }
    }
    static async getAssetHashById(id: Uint256, connection?: pq.TransactionSql): Promise<AssetId | null> {
        const sql = connection || this.connection;
        const result = await this.resultOf(sql`SELECT hash FROM assets WHERE id = ${id.toString()}`);
        try {
            return new AssetId(new Uint8Array(result[0]['hash']));
        } catch {
            return null;
        }
    }
    static async eraseGarbageAssets(connection?: pq.TransactionSql): Promise<number> {
        const sql = connection || this.connection;
        const result = await this.resultOf(sql`DELETE FROM assets WHERE
            NOT EXISTS (SELECT TRUE FROM pairs WHERE primary_asset_id IS NOT NULL AND secondary_asset_id IS NOT NULL AND (assets.id IN (primary_asset_id, secondary_asset_id)))
            AND NOT EXISTS (SELECT TRUE FROM balances WHERE asset_id = assets.id)
            AND NOT EXISTS (SELECT TRUE FROM poly_assets WHERE assets.id IN (asset_id, poly_asset_id))
            AND NOT EXISTS (SELECT TRUE FROM tiers WHERE asset_id = assets.id)`);
        return result.count;
    }
    static async eraseGarbagePairs(connection?: pq.TransactionSql): Promise<number> {
        const sql = connection || this.connection;
        const result = await this.resultOf(sql`
        DELETE FROM pairs WHERE primary_asset_id IS NOT NULL AND secondary_asset_id IS NOT NULL
            AND NOT EXISTS (SELECT TRUE FROM orders WHERE pair_id = pairs.id)
            AND NOT EXISTS (SELECT TRUE FROM pools WHERE pair_id = pairs.id)
            AND NOT EXISTS (SELECT TRUE FROM trades WHERE pair_id = pairs.id)`);
        return result.count;
    }
    static async precomputeMarketData(connection?: pq.TransactionSql): Promise<void> {
        const sql = connection || this.connection;
        await Promise.all([
            this.resultOf(sql`REFRESH MATERIALIZED VIEW pairs_view`),
            this.resultOf(sql`REFRESH MATERIALIZED VIEW pools_view`),
            this.resultOf(sql`REFRESH MATERIALIZED VIEW delegators_view`),
            this.resultOf(sql`UPDATE pairs pair SET launch_time = (SELECT MIN(time) FROM trades WHERE pair_id IN (pair.id, (SELECT id FROM pairs ppair WHERE ppair.primary_asset_id = pair.primary_asset_id AND ppair.secondary_asset_id IS NULL LIMIT 1), (SELECT id FROM pairs spair WHERE spair.primary_asset_id = pair.secondary_asset_id AND spair.secondary_asset_id IS NULL LIMIT 1))) WHERE pair.launch_time IS NULL`)
        ]);
    }
    static async getPairByAssetIds(primaryAssetId: Uint256 | null, secondaryAssetId: Uint256 | null, marketId: Uint256 | null, createIfNotExists: boolean, connection?: pq.TransactionSql): Promise<Uint256 | null> {
        const sql = connection || this.connection;
        if (marketId != null) {
            const market = await this.resultOf(sql`SELECT TRUE FROM markets WHERE id = ${marketId.toString()}`);
            if (!market.length)
                throw new Error('No such market account found');
            
            if (primaryAssetId != null) {
                const polyPrimaryAssetId = await this.getPolyAssetId(primaryAssetId, marketId, connection);
                if (polyPrimaryAssetId) {
                    primaryAssetId = polyPrimaryAssetId;
                }
            }
            
            if (secondaryAssetId != null) {
                const polySecondaryAssetId = await this.getPolyAssetId(secondaryAssetId, marketId, connection);
                if (polySecondaryAssetId) {
                    secondaryAssetId = polySecondaryAssetId;
                }
            }
        }

        const primary = (primaryAssetId ? primaryAssetId.toCompactHex() : 'NULL');
        const secondary = (secondaryAssetId ? secondaryAssetId.toCompactHex() : 'NULL');
        if (primary == secondary)
            return null;

        const pair = primary + ':' + secondary;
        const cache = this.get(this.cache.assetIdsToPairId, pair);
        if (cache != null)
            return new Uint256(cache);

        let result = await this.resultOf(sql`SELECT id FROM pairs WHERE primary_asset_id ${primaryAssetId ? sql`= ${primaryAssetId.toString()}` : sql`IS NULL`} AND secondary_asset_id ${secondaryAssetId ? sql`= ${secondaryAssetId.toString()}` : sql`IS NULL`}`);
        if (!result.length) {
            if (!createIfNotExists)
                throw new Error('Pair does not exist');

            result = await this.resultOf(sql`INSERT INTO pairs (primary_asset_id, secondary_asset_id) VALUES (${primaryAssetId ? primaryAssetId.toString() : null}, ${secondaryAssetId ? secondaryAssetId.toString() : null}) ON CONFLICT (primary_asset_id, secondary_asset_id) DO NOTHING RETURNING id`);
        }
        
        try {
            const value = new Uint256(result[0]['id']);
            this.set(this.cache.assetIdsToPairId, pair, value.toString());
            return value;
        } catch {
            return null;
        }
    }
    static async getPairById(id: Uint256, connection?: pq.TransactionSql): Promise<Pair | null> {
        const sql = connection || this.connection;
        const cache = this.get(this.cache.assetIdsToPairId, id.toCompactHex());
        if (cache != null) {
            const { p, s } = JSON.parse(cache);
            return {
                id: id,
                primaryAsset: p.id != null && p.hash != null ? { id: new Uint256(p.id), hash: new AssetId(Uint8Array.from(p.hash)) } : null,
                secondaryAsset: s.id != null && s.hash != null ? { id: new Uint256(s.id), hash: new AssetId(Uint8Array.from(s.hash)) } : null
            };
        }
        
        const result = await this.resultOf(sql`
        SELECT
            passet.id AS primary_asset_id,
            passet.hash AS primary_asset_hash,
            sasset.id AS secondary_asset_id,
            sasset.hash AS secondary_asset_hash
        FROM pairs
            LEFT JOIN assets passet ON passet.id = pairs.primary_asset_id
            LEFT JOIN assets sasset ON sasset.id = pairs.secondary_asset_id
        WHERE pairs.id = ${id.toString()}`);
        try {
            const pId = result[0]['primary_asset_id'];
            const pHash = result[0]['primary_asset_hash'];
            const sId = result[0]['secondary_asset_id'];
            const sHash = result[0]['secondary_asset_hash'];
            this.set(this.cache.assetIdsToPairId, id.toCompactHex(), JSON.stringify({
                p: { id: pId != null ? pId.toString() : null, hash: pHash != null ? [...new Uint8Array(pHash)] : null },
                s: { id: sId != null ? sId.toString() : null, hash: sHash != null ? [...new Uint8Array(sHash)] : null }
            }));
            return {
                id: id,
                primaryAsset: pId != null && pHash != null ? { id: new Uint256(pId), hash: new AssetId(new Uint8Array(pHash)) } : null,
                secondaryAsset: sId != null && sHash != null ? { id: new Uint256(sId), hash: new AssetId(new Uint8Array(sHash)) } : null
            };
        } catch {
            return null;
        }
    }
    static async getClosestPairsById(assetId: Uint256, connection?: pq.TransactionSql): Promise<Pair[]> {
        const sql = connection || this.connection;
        const result = await this.resultOf(sql`
        SELECT
            pairs.id,
            passet.id AS primary_asset_id,
            passet.hash AS primary_asset_hash,
            sasset.id AS secondary_asset_id,
            sasset.hash AS secondary_asset_hash
        FROM pairs
            LEFT JOIN assets passet ON passet.id = pairs.primary_asset_id
            LEFT JOIN assets sasset ON sasset.id = pairs.secondary_asset_id
        WHERE pairs.primary_asset_id = ${assetId.toString()} OR pairs.secondary_asset_id = ${assetId.toString()}`);
        try {
            const results: Pair[] = [];
            for (let i = 0; i < result.length; i++) {
                const id = result[i]['id'];
                const pId = result[i]['primary_asset_id'];
                const pHash = result[i]['primary_asset_hash'];
                const sId = result[i]['secondary_asset_id'];
                const sHash = result[i]['secondary_asset_hash'];
                results.push({
                    id: new Uint256(id),
                    primaryAsset: pId != null && pHash != null ? { id: new Uint256(pId), hash: new AssetId(new Uint8Array(pHash)) } : null,
                    secondaryAsset: sId != null && sHash != null ? { id: new Uint256(sId), hash: new AssetId(new Uint8Array(sHash)) } : null
                });
            }
            return results;
        } catch {
            return [];
        }
    }
    static async getPairByAssetHashes(primaryAssetHash: AssetId | null, secondaryAssetHash: AssetId | null, marketId: Uint256 | null, verifyExistence: boolean, createIfNotExists: boolean, connection?: pq.TransactionSql): Promise<Uint256 | null> {
        const primaryAssetId = primaryAssetHash ? await this.getAssetIdByHash(primaryAssetHash, verifyExistence ? 'untrusted' : 'trusted', connection) : null;
        const secondaryAssetId = secondaryAssetHash ? await this.getAssetIdByHash(secondaryAssetHash, verifyExistence ? 'untrusted' : 'trusted', connection) : null;
        return await this.getPairByAssetIds(primaryAssetId, secondaryAssetId, marketId, createIfNotExists, connection);
    }
    static async getAggregatedPairs(marketId: Uint256, pairId: Uint256 | null, connection?: pq.TransactionSql): Promise<AggregatedPair[]> {
        const sql = connection || this.connection;
        const result = await this.resultOf(sql`
        WITH results AS (
            SELECT
                pairs_view.*,
                pairs.*,
                passet.hash AS primary_asset,
                sasset.hash AS secondary_asset,
                COALESCE((SELECT SUM(last_price * last_quantity) FROM orders WHERE market_id = ${marketId.toString()} AND pair_id = pairs.id AND active = TRUE), 0.0) AS order_liquidity,
                COALESCE((SELECT SUM(COALESCE(last_ask_price, 0) * primary_value + secondary_value) FROM pools WHERE market_id = ${marketId.toString()} AND pair_id = pairs.id AND active = TRUE), 0.0) AS pool_liquidity,
                COALESCE((SELECT AVG(fee_rate) FROM pools WHERE market_id = ${marketId.toString()} AND pair_id = pairs.id AND active = TRUE), 0.0) AS pool_fee_rate,
                (SELECT ARRAY[price, time] FROM trades WHERE pair_id = pairs.id ORDER BY time DESC LIMIT 1) AS close_price_and_time,
                (SELECT ARRAY[price, time] FROM trades WHERE pair_id = ppair.id ORDER BY time DESC LIMIT 1) AS psynthetic_close_price_and_time,
                (SELECT ARRAY[price, time] FROM trades WHERE pair_id = spair.id ORDER BY time DESC LIMIT 1) AS ssynthetic_close_price_and_time
            FROM pairs
                LEFT JOIN pairs_view ON pairs_view.id = pairs.id
                LEFT JOIN pairs ppair ON ppair.primary_asset_id = pairs.primary_asset_id AND ppair.secondary_asset_id IS NULL
                LEFT JOIN pairs spair ON spair.primary_asset_id = pairs.secondary_asset_id AND spair.secondary_asset_id IS NULL
                INNER JOIN assets passet ON passet.id = pairs.primary_asset_id
                INNER JOIN assets sasset ON sasset.id = pairs.secondary_asset_id
            ${pairId ? sql`WHERE pairs.id = ${pairId.toString()}` : sql``}
        )
        SELECT * FROM results ORDER BY order_liquidity + order_volume + pool_liquidity + pool_volume DESC`);
        try {
            let results: AggregatedPair[] = [];
            for (let i = 0; i < result.length; i++) {
                const item = result[i];
                const secondaryAsset = new AssetId(new Uint8Array(item['secondary_asset']));
                const orderLiquidity = Common.bn(item['order_liquidity']) || null;
                const poolLiquidity = Common.bn(item['pool_liquidity']) || null;
                const orderVolume = Common.bn(item['order_volume']) || null;
                const poolVolume = Common.bn(item['pool_volume']) || null;
                const poolFeeRate = Common.bn(item['pool_fee_rate']) || null;
                const openPriceAndTime = item['open_price_and_time'];
                const openPrice = Common.bn(this.toNull(Array.isArray(openPriceAndTime) && openPriceAndTime.length == 2 ? openPriceAndTime[0] : null)) || null;
                const openTime = Common.num(this.toNull(Array.isArray(openPriceAndTime) && openPriceAndTime.length == 2 ? openPriceAndTime[1] : null)) || 0;
                const closePriceAndTime = item['close_price_and_time'];
                const closePrice = Common.bn(this.toNull(Array.isArray(closePriceAndTime) && closePriceAndTime.length == 2 ? closePriceAndTime[0] : null)) || null;
                const closeTime = Common.num(this.toNull(Array.isArray(closePriceAndTime) && closePriceAndTime.length == 2 ? closePriceAndTime[1] : null)) || 0;
                const lowPriceAndHighPrice = item['low_price_and_high_price'];
                const lowPrice = Common.bn(this.toNull(Array.isArray(lowPriceAndHighPrice) && lowPriceAndHighPrice.length == 2 ? lowPriceAndHighPrice[0] : null)) || null;
                const highPrice = Common.bn(this.toNull(Array.isArray(lowPriceAndHighPrice) && lowPriceAndHighPrice.length == 2 ? lowPriceAndHighPrice[1] : null)) || null;
                const pSyntheticOpenPriceAndTime = item['psynthetic_open_price_and_time'];
                const pSyntheticOpenPrice = Common.bn(this.toNull(Array.isArray(pSyntheticOpenPriceAndTime) && pSyntheticOpenPriceAndTime.length == 2 ? pSyntheticOpenPriceAndTime[0] : null)) || null;
                const pSyntheticOpenTime = Common.num(this.toNull(Array.isArray(pSyntheticOpenPriceAndTime) && pSyntheticOpenPriceAndTime.length == 2 ? pSyntheticOpenPriceAndTime[1] : null)) || null;
                const sSyntheticOpenPriceAndTime = item['ssynthetic_open_price_and_time'];
                const sSyntheticOpenPrice = Common.bn(this.toNull(Array.isArray(sSyntheticOpenPriceAndTime) && sSyntheticOpenPriceAndTime.length == 2 ? sSyntheticOpenPriceAndTime[0] : null)) || null;
                const sSyntheticOpenTime = Common.num(this.toNull(Array.isArray(sSyntheticOpenPriceAndTime) && sSyntheticOpenPriceAndTime.length == 2 ? sSyntheticOpenPriceAndTime[1] : null)) || null;
                const syntheticOpenPrice = pSyntheticOpenPrice && sSyntheticOpenPrice ? pSyntheticOpenPrice.dividedBy(sSyntheticOpenPrice) : null;
                const syntheticOpenTime = pSyntheticOpenTime && sSyntheticOpenTime ? (pSyntheticOpenTime + sSyntheticOpenTime) / 2 : Number.MAX_SAFE_INTEGER;
                const pSyntheticLowPriceAndHighPrice = item['psynthetic_low_price_and_high_price'];
                const pSyntheticLowPrice = Common.bn(this.toNull(Array.isArray(pSyntheticLowPriceAndHighPrice) && pSyntheticLowPriceAndHighPrice.length == 2 ? pSyntheticLowPriceAndHighPrice[0] : null)) || null;
                const pSyntheticHighPrice = Common.bn(this.toNull(Array.isArray(pSyntheticLowPriceAndHighPrice) && pSyntheticLowPriceAndHighPrice.length == 2 ? pSyntheticLowPriceAndHighPrice[1] : null)) || null;
                const sSyntheticLowPriceAndHighPrice = item['ssynthetic_low_price_and_high_price'];
                const sSyntheticLowPrice = Common.bn(this.toNull(Array.isArray(sSyntheticLowPriceAndHighPrice) && sSyntheticLowPriceAndHighPrice.length == 2 ? sSyntheticLowPriceAndHighPrice[0] : null)) || null;
                const sSyntheticHighPrice = Common.bn(this.toNull(Array.isArray(sSyntheticLowPriceAndHighPrice) && sSyntheticLowPriceAndHighPrice.length == 2 ? sSyntheticLowPriceAndHighPrice[1] : null)) || null;
                const pSyntheticClosePriceAndTime = item['psynthetic_close_price_and_time'];
                const pSyntheticClosePrice = Common.bn(this.toNull(Array.isArray(pSyntheticClosePriceAndTime) && pSyntheticClosePriceAndTime.length == 2 ? pSyntheticClosePriceAndTime[0] : null)) || null;
                const pSyntheticCloseTime = Common.num(this.toNull(Array.isArray(pSyntheticClosePriceAndTime) && pSyntheticClosePriceAndTime.length == 2 ? pSyntheticClosePriceAndTime[1] : null)) || null;
                const sSyntheticClosePriceAndTime = item['ssynthetic_close_price_and_time'];
                const sSyntheticClosePrice = Common.bn(this.toNull(Array.isArray(sSyntheticClosePriceAndTime) && sSyntheticClosePriceAndTime.length == 2 ? sSyntheticClosePriceAndTime[0] : null)) || null;
                const sSyntheticCloseTime = Common.num(this.toNull(Array.isArray(sSyntheticClosePriceAndTime) && sSyntheticClosePriceAndTime.length == 2 ? sSyntheticClosePriceAndTime[1] : null)) || null;
                const syntheticClosePrice = pSyntheticClosePrice && sSyntheticClosePrice ? pSyntheticClosePrice.dividedBy(sSyntheticClosePrice) : null;
                const syntheticCloseTime = pSyntheticCloseTime && sSyntheticCloseTime ? (pSyntheticCloseTime + sSyntheticCloseTime) / 2 : Number.MIN_SAFE_INTEGER;
                const syntheticLowPrice = pSyntheticLowPrice != null && sSyntheticLowPrice != null ? pSyntheticLowPrice.div(sSyntheticLowPrice) : null;
                const syntheticHighPrice = pSyntheticHighPrice != null && sSyntheticHighPrice != null ? pSyntheticHighPrice.div(sSyntheticHighPrice) : null;
                const price = syntheticClosePrice && closePrice ? (syntheticCloseTime > closeTime ? syntheticClosePrice : closePrice) : (syntheticClosePrice || closePrice);
                const base = Whitelist.has(secondaryAsset) ? Quotes.assetBaseOf(secondaryAsset) : null;
                results.push({
                    id: Common.u256(item['id']) || new Uint256(),
                    primaryAsset: new AssetId(new Uint8Array(item['primary_asset'])).id,
                    secondaryAsset: secondaryAsset.id,
                    secondaryBase: base,
                    launchTime: Common.num(item['launch_time']) || new Date().getTime(),
                    poolFeeRate: poolFeeRate,
                    price: {
                        orderLiquidity: orderLiquidity,
                        poolLiquidity: poolLiquidity,
                        totalLiquidity: orderLiquidity || poolLiquidity ? (orderLiquidity || new BigNumber(0)).plus(poolLiquidity || new BigNumber(0)) : null,
                        orderVolume: orderVolume,
                        poolVolume: poolVolume,
                        totalVolume: orderVolume || poolVolume ? (orderVolume || new BigNumber(0)).plus(poolVolume || new BigNumber(0)) : null,
                        open: syntheticOpenPrice && openPrice ? (syntheticOpenTime < openTime ? syntheticOpenPrice : openPrice) : (syntheticOpenPrice || openPrice || price),
                        low: syntheticLowPrice && lowPrice ? BigNumber.min(syntheticLowPrice, lowPrice) : (syntheticLowPrice || lowPrice || price),
                        high: syntheticHighPrice && highPrice ? BigNumber.max(syntheticHighPrice, highPrice) : (syntheticHighPrice || highPrice || price),
                        close: price
                    }
                });
            }
            return results;
        } catch {
            return [];
        }
    }
    static async getAssetsByQuery(query: string, connection?: pq.TransactionSql): Promise<AssetId[]> {
        const sql = connection || this.connection;
        const sensitiveTerm = query.toString().trim();
        const term = sensitiveTerm.toUpperCase();
        const queryByChain = Buffer.from(`${term}`, 'utf8').toString('hex') + '%';
        const queryByToken = '%' + Buffer.from(`:${term}`, 'utf8').toString('hex') + '%';
        const queryByTokenSensitive = '%' + Buffer.from(`:${sensitiveTerm}`, 'utf8').toString('hex') + '%';
        const queryByChecksum = '%' + Buffer.from(`:${Hashing.atca160ascii(sensitiveTerm).substring(0, 13)}`, 'utf8').toString('hex') + '%';
        const result = await this.resultOf(sql`SELECT hash FROM assets WHERE encode(hash, 'hex') LIKE ANY(ARRAY[${queryByChain}, ${queryByToken}, ${queryByTokenSensitive}, ${queryByChecksum}]) ORDER BY hash ASC`);
        try {
            return result.map((value) => new AssetId(new Uint8Array(value['hash'])));
        } catch {
            return [];
        }
    }
    static async getAssetPrices(connection?: pq.TransactionSql): Promise<PriceDescriptors> {
        const sql = connection || this.connection;
        const BASENAME = '__BASE__';
        const result = await this.resultOf(sql`
        WITH timings AS (
            SELECT
                (SELECT EXTRACT(EPOCH FROM CURRENT_DATE::TIMESTAMP)::BIGINT * 1000) AS min_time,
                (SELECT EXTRACT(EPOCH FROM CURRENT_DATE::TIMESTAMP + INTERVAL '1 day')::BIGINT * 1000) AS max_time
        )
        SELECT
            assets.hash,
            (SELECT price FROM trades WHERE pair_id = pairs.id AND time >= timings.min_time ORDER BY time ASC LIMIT 1) AS open_price,
            (SELECT price FROM trades WHERE pair_id = pairs.id ORDER BY time DESC LIMIT 1) AS close_price
        FROM assets
            INNER JOIN pairs ON pairs.primary_asset_id = assets.id AND pairs.secondary_asset_id IS NULL
            INNER JOIN timings ON TRUE`);
        try {
            const results: PriceDescriptors = { };
            for (let i = 0; i < result.length; i++) {
                const item = result[i];
                const asset = new AssetId(new Uint8Array(item['hash']));
                const symbol = symbolOf(asset);
                const prev = results[symbol];
                if (prev != null && prev.whitelist)
                    continue;

                const whitelist = Whitelist.has(asset);
                const base = whitelist ? Quotes.baseOf(symbol) : null;
                results[symbol] = {
                    whitelist: whitelist,
                    base: base,
                    price: {
                        open: Common.bn(item['open_price']) || (base ? new BigNumber(1.0) : null),
                        close: Common.bn(item['close_price']) || (base ? new BigNumber(1.0) : null)
                    }
                };
            }
            for (let i = 0; i < Quotes.currencies.length; i++) {
                const currencies = Quotes.currencies[i];
                if (!currencies.length)
                    continue;
                
                if (!results[BASENAME]) {
                    results[BASENAME] = {
                        whitelist: true,
                        base: currencies[0],
                        price: {
                            open: new BigNumber(1.0),
                            close: new BigNumber(1.0)
                        }
                    };
                }
                for (let j = 1; j < currencies.length; j++) {
                    const symbol = currencies[j];
                    if (!results[symbol]) {
                        results[symbol] = {
                            whitelist: true,
                            base: Quotes.globalBase(),
                            price: {
                                open: new BigNumber(1.0),
                                close: new BigNumber(1.0)
                            }
                        };
                    }
                }
            }
            return results;
        } catch {
            return { };
        }
    }
    static async getAssetPrice(primaryAssetId: Uint256, secondaryAssetId: Uint256, connection?: pq.TransactionSql): Promise<BigNumber | null> {
        if (primaryAssetId.toString() == secondaryAssetId.toString())
            return new BigNumber(1);

        const sql = connection || this.connection;
        const result = await this.resultOf(sql`
        SELECT
            (SELECT price FROM trades WHERE pair_id = ppair.id ORDER BY time DESC LIMIT 1) AS primary_price,
            (SELECT price FROM trades WHERE pair_id = spair.id ORDER BY time DESC LIMIT 1) AS secondary_price,
            passet.hash AS primary_asset,
            sasset.hash AS secondary_asset
        FROM assets passet
            INNER JOIN assets sasset ON sasset.id = ${secondaryAssetId.toString()}
            LEFT JOIN pairs ppair ON ppair.primary_asset_id = passet.id AND ppair.secondary_asset_id IS NULL
            LEFT JOIN pairs spair ON spair.primary_asset_id = sasset.id AND ppair.secondary_asset_id IS NULL
        WHERE passet.id = ${primaryAssetId.toString()}`);
        if (!result.length)
            throw new Error('Pair not found');

        const primaryAsset = new AssetId(new Uint8Array(result[0]['primary_asset']));
        const primaryStable = Quotes.baseOf(symbolOf(primaryAsset)) != null;
        const primaryPrice = Common.bn(result[0]['primary_price']) || (primaryStable ? new BigNumber(1) : new BigNumber(NaN));
        const secondaryAsset = new AssetId(new Uint8Array(result[0]['secondary_asset']));
        const secondaryStable = Quotes.baseOf(symbolOf(secondaryAsset)) != null;
        const secondaryPrice = Common.bn(result[0]['secondary_price']) || (secondaryStable ? new BigNumber(1) : new BigNumber(NaN));
        const price = primaryPrice.dividedBy(secondaryPrice);
        if (!price.isFinite())
            throw new Error('Price not found');

        return price;
    }
    static async getAssetTWAP(primaryAssetId: Uint256, secondaryAssetId: Uint256, interval: number, connection?: pq.TransactionSql): Promise<BigNumber | null> {
        if (primaryAssetId.toString() == secondaryAssetId.toString()) {
            return new BigNumber(1);
        } else if (interval < 0) {
            return null;
        }

        const sql = connection || this.connection;
        const result = await this.resultOf(sql`
        WITH points AS (
            SELECT
                (SELECT time FROM trades WHERE pair_id = ppair.id ORDER BY time DESC LIMIT 1) AS primary_time,
                (SELECT time FROM trades WHERE pair_id = spair.id ORDER BY time DESC LIMIT 1) AS secondary_time,
                ppair.id AS primary_pair,
                spair.id AS secondary_pair,
                passet.hash AS primary_asset,
                sasset.hash AS secondary_asset
            FROM assets passet
                INNER JOIN assets sasset ON sasset.id = ${secondaryAssetId.toString()}
                LEFT JOIN pairs ppair ON ppair.primary_asset_id = passet.id AND ppair.secondary_asset_id IS NULL
                LEFT JOIN pairs spair ON spair.primary_asset_id = sasset.id AND ppair.secondary_asset_id IS NULL
            WHERE passet.id = ${primaryAssetId.toString()}
        )
        SELECT
            (SELECT SUM(price * GREATEST(quantity, 0.000001)) / SUM(GREATEST(quantity, 0.000001)) FROM trades WHERE pair_id = points.primary_pair AND time >= points.primary_time - ${interval}) AS primary_price,
            (SELECT SUM(price * GREATEST(quantity, 0.000001)) / SUM(GREATEST(quantity, 0.000001)) FROM trades WHERE pair_id = points.secondary_pair AND time >= points.secondary_time - ${interval}) AS secondary_price,
            points.primary_asset,
            points.secondary_asset
        FROM points`);
        if (!result.length)
            throw new Error('Pair not found');

        const primaryAsset = new AssetId(new Uint8Array(result[0]['primary_asset']));
        const primaryStable = Quotes.baseOf(symbolOf(primaryAsset)) != null;
        const primaryPrice = Common.bn(result[0]['primary_price']) || (primaryStable ? new BigNumber(1) : new BigNumber(NaN));
        const secondaryAsset = new AssetId(new Uint8Array(result[0]['secondary_asset']));
        const secondaryStable = Quotes.baseOf(symbolOf(secondaryAsset)) != null;
        const secondaryPrice = Common.bn(result[0]['secondary_price']) || (secondaryStable ? new BigNumber(1) : new BigNumber(NaN));
        const price = primaryPrice.dividedBy(secondaryPrice);
        if (!price.isFinite())
            throw new Error('Price not found');

        return price;
    }
    static async getAssetHandles(connection?: pq.TransactionSql): Promise<AssetId[]> {
        const sql = connection || this.connection;
        const result = await this.resultOf(sql`SELECT hash FROM assets`);
        try {
            const results: AssetId[] = [];
            for (let i = 0; i < result.length; i++) {
                results.push(new AssetId(new Uint8Array(result[i]['hash'])));
            }
            return results;
        } catch {
            return [];
        }
    }
    static async setAccountSyncByAccountIds(ids: Uint256[], synced: boolean, connection?: pq.TransactionSql): Promise<void> {
        const sql = connection || this.connection;
        await this.resultOf(sql`UPDATE accounts SET synced = ${synced} WHERE id = ANY(${ids.map((x) => x.toString())})`);
    }
    static async getAccountSyncByAccountId(id: Uint256, connection?: pq.TransactionSql): Promise<boolean> {
        const sql = connection || this.connection;
        const result = await this.resultOf(sql`SELECT synced FROM accounts WHERE id = ${id.toString()}`);
        try {
            return !!result[0]['synced'];
        } catch {
            return false;
        }
    }
    static async getAccountIdByHash(hash: Pubkeyhash, onlyIfExists: boolean, connection?: pq.TransactionSql): Promise<Uint256 | null> {
        const sql = connection || this.connection;
        const cache = this.get(this.cache.accountHashToAssetId, ByteUtil.uint8ArrayToHexString(hash.data));
        if (cache != null)
            return new Uint256(cache);

        let result = await this.resultOf(sql`SELECT id FROM accounts WHERE hash = ${hash.data}`);
        if (!onlyIfExists && !result.length)
            result = await this.resultOf(sql`INSERT INTO accounts (hash) VALUES (${hash.data}) ON CONFLICT (hash) DO NOTHING RETURNING id`);
        try {
            const value = new Uint256(result[0]['id']);
            this.set(this.cache.accountHashToAssetId, ByteUtil.uint8ArrayToHexString(hash.data), value.toString());
            return value;
        } catch {
            return null;
        }
    }
    static async getAccountIdByAddress(address: string, onlyIfExists: boolean, connection?: pq.TransactionSql): Promise<Uint256 | null> {
        const hash = Signing.decodeAddress(address);
        if (!hash)
            return null;

        return this.getAccountIdByHash(hash, onlyIfExists, connection);
    }
    static async getAccountHashById(id: Uint256, connection?: pq.TransactionSql): Promise<Pubkeyhash | null> {
        const sql = connection || this.connection;
        const result = await this.resultOf(sql`SELECT hash FROM accounts WHERE id = ${id.toString()}`);
        try {
            return new Pubkeyhash(new Uint8Array(result[0]['hash']));
        } catch {
            return null;
        }
    }
    static async setSyncedAccountBalancesByAccountId(id: Uint256, time: Date, balances: { asset: AssetId, value: BigNumber }[], connection?: pq.TransactionSql): Promise<void> {
        const sql = connection || this.connection;
        const top = await this.getLatestBlock(undefined, connection);
        const updates: { account_id: string, asset_id: string, block_number: number, time: number, value: string }[] = [];
        for (let i = 0; i < balances.length; i++) {
            const balance = balances[i];
            const assetId = await this.getAssetIdByHash(balance.asset, 'trusted', connection);
            if (assetId != null && balance.value.gt(0)) {
                const asset = assetId.toInteger().toString();
                updates.push({ account_id: id.toString(), asset_id: asset, block_number: top?.blockNumber || 1, time: time.getTime(), value: balance.value.toString() });
            }
        }

        await Promise.all([
            this.resultOf(sql`UPDATE accounts SET synced = TRUE WHERE id = ${id.toString()}`),
            this.resultOf(sql`UPDATE balances SET value = 0 WHERE account_id = ${id.toString()}`)
        ]);
        if (updates.length > 0) {
            await this.resultOf(sql`INSERT INTO balances ${sql(updates, 'account_id', 'asset_id', 'block_number', 'time', 'value')} ON CONFLICT (account_id, asset_id) DO UPDATE SET
                block_number = EXCLUDED.block_number,
                time = LEAST(balances.time, EXCLUDED.time),
                value = EXCLUDED.value`);
        }
        await this.resultOf(sql`DELETE FROM balances WHERE account_id = ${id.toString()} AND value <= 0`);
    }
    static async getSyncedAccountBalancesByAccountId(id: Uint256, connection?: pq.TransactionSql): Promise<{ asset: AssetId, unavailable: BigNumber, available: BigNumber, price: BigNumber | null }[] | null> {
        const sql = connection || this.connection;
        const synced = await this.getAccountSyncByAccountId(id, connection);
        if (!synced)
            return null;

        const result = await this.resultOf(sql`
        WITH weights AS (
            WITH best AS (
                SELECT DISTINCT
                    id1 AS account_id,
                    id2 AS asset_id,
                    COALESCE((SELECT MAX(time) FROM balances WHERE account_id = id1 AND asset_id = id2), (extract(epoch from now()) * 1000)::BIGINT) AS time
                FROM (
                    SELECT DISTINCT account_id AS id1, asset_id AS id2 FROM balances WHERE account_id = ${id.toString()}
                    UNION ALL
                    SELECT DISTINCT account_id AS id1, unnest(ARRAY[primary_asset_id, secondary_asset_id]) AS id2 FROM orders
                        INNER JOIN pairs ON pairs.id = orders.pair_id
                    WHERE account_id = ${id.toString()} AND active = TRUE
                    UNION ALL
                    SELECT DISTINCT account_id AS id1, unnest(ARRAY[primary_asset_id, secondary_asset_id]) AS id2 FROM pools
                        INNER JOIN pairs ON pairs.id = pools.pair_id
                    WHERE account_id = ${id.toString()} AND active = TRUE
                    UNION ALL
                    SELECT DISTINCT account_id AS id1, unnest(ARRAY[primary_asset_id, secondary_asset_id]) AS id2 FROM delegated_pools
                        INNER JOIN pairs ON pairs.id = delegated_pools.pair_id
                    WHERE account_id = ${id.toString()} AND active = TRUE
                )
            )
            SELECT
                best.asset_id, COALESCE(balances.value, 0) AS value,
                (SELECT price FROM trades WHERE pair_id = pairs.id AND trades.time BETWEEN extract(epoch FROM date_trunc('day', to_timestamp(balances.time / 1000))) * 1000 AND balances.time ORDER BY time DESC LIMIT 1) AS target_price,
                (SELECT price FROM trades WHERE pair_id = pairs.id ORDER BY time ASC LIMIT 1) AS fallback_price
            FROM best
                LEFT JOIN balances ON balances.account_id = best.account_id AND balances.asset_id = best.asset_id AND balances.value > 0
                LEFT JOIN pairs ON pairs.primary_asset_id = best.asset_id AND pairs.secondary_asset_id IS NULL
        )
        SELECT
            (SELECT hash FROM assets WHERE assets.id = asset_id) AS hash,
            (
                SELECT COALESCE(SUM(value), 0.0) FROM orders
                    INNER JOIN pairs ON pairs.id = orders.pair_id AND ((pairs.primary_asset_id = asset_id AND orders.side = ${OrderSide.Sell}) OR (pairs.secondary_asset_id = asset_id AND orders.side = ${OrderSide.Buy}))
                WHERE account_id = ${id.toString()} AND active = TRUE
            ) +
            (
                SELECT
                    COALESCE(SUM(CASE WHEN pairs.primary_asset_id = asset_id THEN primary_value + primary_revenue ELSE secondary_value + secondary_revenue END), 0.0)
                FROM pools
                    INNER JOIN pairs ON pairs.id = pools.pair_id AND (pairs.primary_asset_id = asset_id OR pairs.secondary_asset_id = asset_id)
                WHERE account_id = ${id.toString()} AND active = TRUE
            ) +
            (
                SELECT
                    COALESCE(SUM(CASE WHEN pairs.primary_asset_id = asset_id THEN primary_value ELSE secondary_value END), 0.0)
                FROM delegated_pools
                    INNER JOIN pairs ON pairs.id = delegated_pools.pair_id AND (pairs.primary_asset_id = asset_id OR pairs.secondary_asset_id = asset_id)
                WHERE account_id = ${id.toString()} AND active = TRUE
            ) AS unavailable,
            value AS available,
            COALESCE(target_price, fallback_price) AS price
        FROM weights`);
        try {
            let balances = [];
            for (let i = 0; i < result.length; i++) {
                const balance = result[i];
                const asset = new AssetId(new Uint8Array(balance['hash']));
                const base = Quotes.assetBaseOf(asset);
                balances.push({
                    asset: asset,
                    unavailable: new BigNumber(balance['unavailable'] || 0),
                    available: new BigNumber(balance['available'] || 0),
                    price: Common.bn(balance['price']) || (base ? new BigNumber(1.0) : null),
                });
            }
            return balances.filter((v) => v.available.gt(0) || v.unavailable.gt(0));
        } catch {
            return [];
        }
    }
    static async setSyncedAccountTierByAccountIdAndMarketAsset(accountId: Uint256, marketId: Uint256, assetId: Uint256, volume: BigNumber, makerFee: BigNumber, takerFee: BigNumber, connection?: pq.TransactionSql): Promise<void> {
        const sql = connection || this.connection;
        const top = await this.getLatestBlock(undefined, connection);
        await this.resultOf(sql`
        INSERT INTO tiers
        (
            account_id,
            asset_id,
            market_id,
            block_number,
            volume,
            maker_fee,
            taker_fee
        )
        VALUES
        (
            ${accountId.toString()},
            ${assetId.toString()},
            ${marketId.toString()},
            ${top?.blockNumber || 1},
            ${volume.toString()},
            ${makerFee.toString()},
            ${takerFee.toString()}
        )
        ON CONFLICT (account_id, asset_id, market_id) DO UPDATE SET
            block_number = EXCLUDED.block_number,
            volume = EXCLUDED.volume,
            maker_fee = EXCLUDED.maker_fee,
            taker_fee = EXCLUDED.taker_fee`);
    }
    static async getSyncedAccountTierByAccountIdAndMarketAsset(accountId: Uint256, marketId: Uint256, assetId: Uint256, connection?: pq.TransactionSql): Promise<{ volume: BigNumber | null, makerFee: BigNumber | null, takerFee: BigNumber | null } | null> {
        const sql = connection || this.connection;
        const synced = await this.getAccountSyncByAccountId(accountId, connection);
        if (!synced)
            return null;

        const result = await this.resultOf(sql`SELECT * FROM tiers WHERE account_id = ${accountId.toString()} AND asset_id = ${assetId.toString()} AND market_id = ${marketId.toString()}`);
        if (!result.length)
            return null;

        return {
            volume: Common.bn(result[0]['volume']) || null,
            makerFee: Common.bn(result[0]['maker_fee']) || null,
            takerFee: Common.bn(result[0]['taker_fee']) || null
        };
    }
    static async getOverallMetrics(connection?: pq.TransactionSql): Promise<{ assets: number, pairs: number, accounts: number, actions: number, quantity: BigNumber, volume: BigNumber } | null> {
        const sql = connection || this.connection;
        const result = await this.resultOf(sql`
        SELECT
            (SELECT COUNT(1) FROM assets) AS assets,
            (SELECT COUNT(1) FROM pairs WHERE primary_asset_id IS NOT NULL AND secondary_asset_id IS NOT NULL) AS pairs,
            (SELECT COUNT(1) FROM accounts) AS accounts,
            (SELECT (SELECT COUNT(1) FROM orders) + (SELECT COUNT(1) FROM pools)) AS actions,
            (
                WITH aggregations AS (
                    SELECT
                        (SELECT price FROM trades WHERE pair_id = (SELECT id FROM pairs WHERE primary_asset_id = asset_id AND secondary_asset_id IS NULL) ORDER BY time DESC LIMIT 1) AS price,
                        SUM(value) AS value
                    FROM balances GROUP BY asset_id
                )
                SELECT SUM(value * COALESCE(price, 0)) FROM aggregations
            ) AS quantity,
            (
                WITH aggregations AS (
                    SELECT
                        (SELECT trades.price FROM trades WHERE trades.pair_id = (SELECT id FROM pairs WHERE primary_asset_id = (SELECT primary_asset_id FROM pairs WHERE pairs.id = depths.pair_id) AND secondary_asset_id IS NULL) ORDER BY time DESC LIMIT 1) AS price,
                        SUM(quantity) AS value
                    FROM depths WHERE quantity > 0 GROUP BY pair_id
                    UNION ALL
                    SELECT
                        (SELECT trades.price FROM trades WHERE trades.pair_id = (SELECT t.id FROM pairs t WHERE t.primary_asset_id = pairs.primary_asset_id AND t.secondary_asset_id IS NULL) ORDER BY time DESC LIMIT 1) AS price,
                        (SELECT SUM(quantity) FROM trades WHERE pair_id = pairs.id) AS value
                    FROM pairs WHERE primary_asset_id IS NOT NULL AND secondary_asset_id IS NOT NULL
                )
                SELECT SUM(value * COALESCE(price, 0)) FROM aggregations
            ) AS volume`);
        try {
            const value = result[0];
            return {
                assets: Common.num(value['assets']) || 0,
                pairs: Common.num(value['pairs']) || 0,
                accounts: Common.num(value['accounts']) || 0,
                actions: Common.num(value['actions']) || 0,
                quantity: Common.bn(value['quantity']) || new BigNumber(0),
                volume: Common.bn(value['volume']) || new BigNumber(0)
            };
        } catch {
            return null;
        }
    }
    static async setMarket(market: Omit<Market, 'id'>, connection?: pq.TransactionSql): Promise<Market | null> {
        const sql = connection || this.connection;
        const result = await this.resultOf(sql`
        INSERT INTO markets
        (
            account_id,
            deployer_account_id,
            block_number,
            pool_exit_fee,
            max_pool_fee_rate,
            min_maker_fee,
            max_maker_fee,
            maker_fee_exponent,
            min_taker_fee,
            max_taker_fee,
            taker_fee_exponent,
            asset_volume_target,
            asset_reset_days,
            account_reset_days,
            market_policy
        )
        VALUES
        (
            ${market.accountId.toString()},
            ${market.deployerAccountId.toString()},
            ${market.blockNumber},
            ${market.poolExitFee.toString()},
            ${market.maxPoolFeeRate.toString()},
            ${market.minMakerFee.toString()},
            ${market.maxMakerFee.toString()},
            ${market.makerFeeExponent},
            ${market.minTakerFee.toString()},
            ${market.maxTakerFee.toString()},
            ${market.takerFeeExponent},
            ${market.assetVolumeTarget.toString()},
            ${market.assetResetDays},
            ${market.accountResetDays},
            ${market.marketPolicy}
        )
        ON CONFLICT (account_id) DO UPDATE SET
            deployer_account_id = EXCLUDED.deployer_account_id,
            block_number = EXCLUDED.block_number,
            pool_exit_fee = EXCLUDED.pool_exit_fee,
            max_pool_fee_rate = EXCLUDED.max_pool_fee_rate,
            min_maker_fee = EXCLUDED.min_maker_fee,
            max_maker_fee = EXCLUDED.max_maker_fee,
            maker_fee_exponent = EXCLUDED.maker_fee_exponent,
            min_taker_fee = EXCLUDED.min_taker_fee,
            max_taker_fee = EXCLUDED.max_taker_fee,
            taker_fee_exponent = EXCLUDED.taker_fee_exponent,
            asset_volume_target = EXCLUDED.asset_volume_target,
            asset_reset_days = EXCLUDED.asset_reset_days,
            account_reset_days = EXCLUDED.account_reset_days,
            market_policy = EXCLUDED.market_policy
        RETURNING *`);
        try {
            return this.toMarket(result[0]);
        } catch {
            return null;
        }
    }
    static async getMarketById(id: Uint256, connection?: pq.TransactionSql): Promise<Market | null> {
        const sql = connection || this.connection;
        const result = await this.resultOf(sql`
        SELECT
            markets.*,
            accounts.hash AS account_hash,
            deployer_accounts.hash AS deployer_account_hash
        FROM markets
            INNER JOIN accounts ON accounts.id = markets.account_id
            INNER JOIN accounts deployer_accounts ON deployer_accounts.id = markets.deployer_account_id
        WHERE markets.id = ${id.toString()}`);
        try {
            return this.toMarket(result[0]);
        } catch {
            return null;
        }
    }
    static async getMarketByAccountId(accountId: Uint256, connection?: pq.TransactionSql): Promise<Market | null> {
        const sql = connection || this.connection;
        const result = await this.resultOf(sql`
        SELECT
            markets.*,
            accounts.hash AS account_hash,
            deployer_accounts.hash AS deployer_account_hash
        FROM markets
            INNER JOIN accounts ON accounts.id = markets.account_id
            INNER JOIN accounts deployer_accounts ON deployer_accounts.id = markets.deployer_account_id
        WHERE account_id = ${accountId.toString()}`);
        try {
            return this.toMarket(result[0]);
        } catch {
            return null;
        }
    }
    static async getMarkets(connection?: pq.TransactionSql): Promise<Market[]> {
        const sql = connection || this.connection;
        const result = await this.resultOf(sql`
        SELECT
            markets.*,
            accounts.hash AS account_hash,
            deployer_accounts.hash AS deployer_account_hash
        FROM markets
            INNER JOIN accounts ON accounts.id = markets.account_id
            INNER JOIN accounts deployer_accounts ON deployer_accounts.id = markets.deployer_account_id`);
        try {
            let results: Market[] = [];
            for (let i = 0; i < result.length; i++)
                results.push(this.toMarket(result[i]));
            return results;
        } catch {
            return [];
        }
    }
    static async getDelegators(connection?: pq.TransactionSql): Promise<Delegator[]> {
        const sql = connection || this.connection;
        const result = await this.resultOf(sql`
        SELECT
            delegators.*,
            accounts.hash AS account_hash,
            deployer_accounts.hash AS deployer_account_hash
        FROM delegators
            INNER JOIN accounts ON accounts.id = delegators.account_id
            INNER JOIN accounts deployer_accounts ON deployer_accounts.id = delegators.deployer_account_id`);
        try {
            let results: Delegator[] = [];
            for (let i = 0; i < result.length; i++)
                results.push(this.toDelegator(result[i]));
            return results;
        } catch {
            return [];
        }
    }
    static async setOrder(order: Omit<Order, 'id'>, connection?: pq.TransactionSql): Promise<Order | null> {
        const sql = connection || this.connection;
        const result = await this.resultOf(sql`
        INSERT INTO orders
        (
            order_id,
            pair_id,
            market_id,
            account_id,
            block_number,
            condition,
            side,
            policy,
            price,
            stop_price,
            filling_price,
            starting_value,
            value,
            slippage,
            trailing_step,
            trailing_distance,
            active
        )
        VALUES
        (
            ${order.orderId.toUint8Array()},
            ${order.pairId.toString()},
            ${order.marketId.toString()},
            ${order.accountId.toString()},
            ${order.blockNumber},
            ${order.condition},
            ${order.side},
            ${order.policy},
            ${order.price?.toString() || null},
            ${order.stopPrice?.toString() || null},
            ${order.fillingPrice?.toString() || null},
            ${order.startingValue.toString()},
            ${order.value.toString()},
            ${order.slippage?.toString() || null},
            ${order.trailingStep?.toString() || null},
            ${order.trailingDistance?.toString() || null},
            ${order.active}
        )
        ON CONFLICT (market_id, order_id) DO UPDATE SET
            pair_id = EXCLUDED.pair_id,
            account_id = EXCLUDED.account_id,
            block_number = EXCLUDED.block_number,
            condition = EXCLUDED.condition,
            side = EXCLUDED.side,
            policy = EXCLUDED.policy,
            price = EXCLUDED.price,
            stop_price = EXCLUDED.stop_price,
            filling_price = (CASE WHEN orders.filling_price IS NOT NULL AND EXCLUDED.filling_price IS NOT NULL THEN (orders.filling_price + EXCLUDED.filling_price) / 2 ELSE COALESCE(orders.filling_price, EXCLUDED.filling_price) END),
            value = EXCLUDED.value,
            slippage = EXCLUDED.slippage,
            trailing_step = EXCLUDED.trailing_step,
            trailing_distance = EXCLUDED.trailing_distance,
            active = EXCLUDED.active
        RETURNING *`);
        try {
            return this.toOrder(result[0]);
        } catch {
            return null;
        }
    }
    static async getOrderById(id: Uint256, connection?: pq.TransactionSql): Promise<Order | null> {
        const sql = connection || this.connection;
        const result = await this.resultOf(sql`
        SELECT
            orders.*,
            passet.hash AS primary_asset,
            sasset.hash AS secondary_asset,
            accounts.hash AS market_account_hash
        FROM orders
            INNER JOIN pairs ON pairs.id = pair_id
            INNER JOIN assets passet ON passet.id = pairs.primary_asset_id
            INNER JOIN assets sasset ON sasset.id = pairs.secondary_asset_id
            INNER JOIN markets ON markets.id = market_id
            INNER JOIN accounts ON accounts.id = markets.account_id
        WHERE orders.id = ${id.toString()}`);
        try {
            return this.toOrder(result[0]);
        } catch {
            return null;
        }
    }
    static async getOrderByOrderId(orderId: Uint256, connection?: pq.TransactionSql): Promise<Order | null> {
        const sql = connection || this.connection;
        const result = await this.resultOf(sql`
        SELECT
            orders.*,
            passet.hash AS primary_asset,
            sasset.hash AS secondary_asset,
            accounts.hash AS market_account_hash
        FROM orders
            INNER JOIN pairs ON pairs.id = pair_id
            INNER JOIN assets passet ON passet.id = pairs.primary_asset_id
            INNER JOIN assets sasset ON sasset.id = pairs.secondary_asset_id
            INNER JOIN markets ON markets.id = market_id
            INNER JOIN accounts ON accounts.id = markets.account_id
        WHERE order_id = ${orderId.toUint8Array()}`);
        try {
            return this.toOrder(result[0]);
        } catch {
            return null;
        }
    }
    static async getOrdersByAccountId(accountId: Uint256, active: boolean | null, cursor: Cursor, connection?: pq.TransactionSql): Promise<Order[]> {
        const sql = connection || this.connection;
        const result = await this.resultOf(sql`
        SELECT
            orders.*,
            passet.hash AS primary_asset,
            sasset.hash AS secondary_asset,
            accounts.hash AS market_account_hash
        FROM orders
            INNER JOIN pairs ON pairs.id = pair_id
            INNER JOIN assets passet ON passet.id = pairs.primary_asset_id
            INNER JOIN assets sasset ON sasset.id = pairs.secondary_asset_id
            INNER JOIN markets ON markets.id = market_id
            INNER JOIN accounts ON accounts.id = markets.account_id
        WHERE orders.account_id = ${accountId.toString()}${typeof active == 'boolean' ? sql` AND active = ${active}` : sql``} ORDER BY active DESC, block_number DESC LIMIT ${cursor.count} OFFSET ${cursor.offset}`);
        try {
            const orders = [];
            for (let i = 0; i < result.length; i++) {
                orders.push(this.toOrder(result[i]));
            }
            return orders;
        } catch {
            return [];
        }
    }
    static async getOrdersByAccountIdAndMarketPair(accountId: Uint256, marketId: Uint256, pairId: Uint256, active: boolean | null, cursor: Cursor, connection?: pq.TransactionSql): Promise<Order[]> {
        const sql = connection || this.connection;
        const result = await this.resultOf(sql`
        SELECT
            orders.*,
            passet.hash AS primary_asset,
            sasset.hash AS secondary_asset,
            accounts.hash AS market_account_hash
        FROM orders
            INNER JOIN pairs ON pairs.id = pair_id
            INNER JOIN assets passet ON passet.id = pairs.primary_asset_id
            INNER JOIN assets sasset ON sasset.id = pairs.secondary_asset_id
            INNER JOIN markets ON markets.id = market_id
            INNER JOIN accounts ON accounts.id = markets.account_id
        WHERE orders.account_id = ${accountId.toString()} AND orders.market_id = ${marketId.toString()} AND orders.pair_id = ${pairId.toString()}${typeof active == 'boolean' ? sql` AND active = ${active}` : sql``} ORDER BY active DESC, block_number DESC LIMIT ${cursor.count} OFFSET ${cursor.offset}`);
        try {
            const orders = [];
            for (let i = 0; i < result.length; i++) {
                orders.push(this.toOrder(result[i]));
            }
            return orders;
        } catch {
            return [];
        }
    }
    static async setPool(pool: Omit<Pool, 'id'>, connection?: pq.TransactionSql): Promise<Pool | null> {
        const sql = connection || this.connection;
        const result = await this.resultOf(sql`
        INSERT INTO pools
        (
            pool_id,
            pair_id,
            market_id,
            account_id,
            block_number,
            initial_price,
            initial_primary_value,
            initial_secondary_value,
            primary_value,
            secondary_value,
            primary_revenue,
            secondary_revenue,
            liquidity,
            price,
            min_price,
            max_price,
            fee_rate,
            exit_fee,
            active
        )
        VALUES
        (
            ${pool.poolId.toUint8Array()},
            ${pool.pairId.toString()},
            ${pool.marketId.toString()},
            ${pool.accountId.toString()},
            ${pool.blockNumber},
            ${pool.initialPrice.toString()},
            ${pool.initialPrimaryValue.toString()},
            ${pool.initialSecondaryValue.toString()},
            ${pool.primaryValue.toString()},
            ${pool.secondaryValue.toString()},
            ${pool.primaryRevenue.toString()},
            ${pool.secondaryRevenue.toString()},
            ${pool.liquidity.toString()},
            ${pool.price.toString()},
            ${pool.minPrice?.toString() || null},
            ${pool.maxPrice?.toString() || null},
            ${pool.feeRate.toString()},
            ${pool.exitFee.toString()},
            ${pool.active}
        )
        ON CONFLICT (market_id, pool_id) DO UPDATE SET
            pair_id = EXCLUDED.pair_id,
            account_id = EXCLUDED.account_id,
            block_number = EXCLUDED.block_number,
            primary_value = EXCLUDED.primary_value,
            secondary_value = EXCLUDED.secondary_value,
            primary_revenue = EXCLUDED.primary_revenue,
            secondary_revenue = EXCLUDED.secondary_revenue,
            liquidity = EXCLUDED.liquidity,
            price = EXCLUDED.price,
            min_price = EXCLUDED.min_price,
            max_price = EXCLUDED.max_price,
            fee_rate = EXCLUDED.fee_rate,
            exit_fee = EXCLUDED.exit_fee,
            active = EXCLUDED.active
        RETURNING *`);
        try {
            return this.toPool(result[0]);
        } catch {
            return null;
        }
    }
    static async getPoolById(id: Uint256, connection?: pq.TransactionSql): Promise<Pool | null> {
        const sql = connection || this.connection;
        const result = await this.resultOf(sql`
        SELECT
            pools.*,
            passet.hash AS primary_asset,
            sasset.hash AS secondary_asset,
            accounts.hash AS market_account_hash,
            pools_view.volume
        FROM pools
            INNER JOIN pairs ON pairs.id = pair_id
            INNER JOIN assets passet ON passet.id = pairs.primary_asset_id
            INNER JOIN assets sasset ON sasset.id = pairs.secondary_asset_id
            INNER JOIN markets ON markets.id = market_id
            INNER JOIN accounts ON accounts.id = markets.account_id
            LEFT JOIN pools_view ON pools_view.id = pools.id
        WHERE pools.id = ${id.toString()}`);
        try {
            return this.toPool(result[0]);
        } catch {
            return null;
        }
    }
    static async getPoolByPoolId(poolId: Uint256, connection?: pq.TransactionSql): Promise<Pool | null> {
        const sql = connection || this.connection;
        const result = await this.resultOf(sql`
        SELECT
            pools.*,
            passet.hash AS primary_asset,
            sasset.hash AS secondary_asset,
            accounts.hash AS market_account_hash,
            pools_view.volume
        FROM pools
            INNER JOIN pairs ON pairs.id = pair_id
            INNER JOIN assets passet ON passet.id = pairs.primary_asset_id
            INNER JOIN assets sasset ON sasset.id = pairs.secondary_asset_id
            INNER JOIN markets ON markets.id = market_id
            INNER JOIN accounts ON accounts.id = markets.account_id
            LEFT JOIN pools_view ON pools_view.id = pools.id
        WHERE pool_id = ${poolId.toUint8Array()}`);
        try {
            return this.toPool(result[0]);
        } catch {
            return null;
        }
    }
    static async getBestPools(cursor: Cursor, connection?: pq.TransactionSql): Promise<Pool[]> {
        const sql = connection || this.connection;
        const result = await this.resultOf(sql`
        SELECT
            pools.*,
            passet.hash AS primary_asset,
            sasset.hash AS secondary_asset,
            accounts.hash AS market_account_hash,
            pools_view.volume
        FROM pools
            INNER JOIN pairs ON pairs.id = pair_id
            INNER JOIN assets passet ON passet.id = pairs.primary_asset_id
            INNER JOIN assets sasset ON sasset.id = pairs.secondary_asset_id
            INNER JOIN markets ON markets.id = market_id
            INNER JOIN accounts ON accounts.id = markets.account_id
            LEFT JOIN pools_view ON pools_view.id = pools.id
        WHERE active = TRUE ORDER BY 100000000 * (primary_revenue * price + secondary_revenue) / (primary_value * price + secondary_value) DESC LIMIT ${cursor.count} OFFSET ${cursor.offset}`);
        try {
            const pools = [];
            for (let i = 0; i < result.length; i++) {
                pools.push(this.toPool(result[i]));
            }
            return pools;
        } catch {
            return [];
        }
    }
    static async getBestDelegatedPools(connection?: pq.TransactionSql): Promise<PseudoDelegatedPool[]> {
        const sql = connection || this.connection;
        const result = await this.resultOf(sql`
        WITH aggregates AS (
            WITH targets AS (
                WITH sources AS (
                    SELECT
                        market_id,
                        pair_id,
                        delegator_id,
                        SUM(initial_primary_value) AS initial_primary_value,
                        SUM(initial_secondary_value) AS initial_secondary_value,
                        SUM(primary_value) AS primary_value,
                        SUM(secondary_value) AS secondary_value
                    FROM delegated_pools WHERE active = TRUE
                    GROUP BY market_id, pair_id, delegator_id
                )
                SELECT
                    sources.*,
                    delegators.account_id,
                    passet.hash AS primary_asset,
                    sasset.hash AS secondary_asset,
                    market_account.hash AS market_account_hash,
                    delegator_account.hash AS delegator_account_hash,
                    (SELECT v.fee_rate FROM pools v WHERE v.pair_id = sources.pair_id AND v.market_id = sources.market_id AND v.account_id = delegators.account_id AND v.active = TRUE LIMIT 1) AS fee_rate,
                    COALESCE((SELECT price FROM trades WHERE pair_id = (SELECT id FROM pairs WHERE primary_asset_id = passet.id AND secondary_asset_id IS NULL) ORDER BY time DESC LIMIT 1), 1) AS primary_price,
                    COALESCE((SELECT price FROM trades WHERE pair_id = (SELECT id FROM pairs WHERE primary_asset_id = sasset.id AND secondary_asset_id IS NULL) ORDER BY time DESC LIMIT 1), 1) AS secondary_price
                FROM sources
                    INNER JOIN pairs ON pairs.id = pair_id
                    INNER JOIN assets passet ON passet.id = pairs.primary_asset_id
                    INNER JOIN assets sasset ON sasset.id = pairs.secondary_asset_id
                    INNER JOIN markets ON markets.id = sources.market_id
                    INNER JOIN delegators ON delegators.id = sources.delegator_id
                    INNER JOIN accounts market_account ON market_account.id = markets.account_id
                    INNER JOIN accounts delegator_account ON delegator_account.id = delegators.account_id
            )
            SELECT
                targets.*,
                initial_primary_value * primary_price + initial_secondary_value * secondary_price AS initial_value,
                primary_value * primary_price + secondary_value * secondary_price AS current_value,
                delegators_view.volume
            FROM targets
                LEFT JOIN delegators_view ON delegators_view.id = targets.delegator_id AND delegators_view.pair_id = targets.pair_id
            WHERE initial_primary_value > 0 OR initial_secondary_value > 0
        )
        SELECT * FROM aggregates ORDER BY 100000000 * (current_value - initial_value) / initial_value DESC`);
        try {
            const delegatedPools: PseudoDelegatedPool[] = [];
            for (let i = 0; i < result.length; i++) {
                const value = result[i];
                delegatedPools.push({
                    pairId: new Uint256(value['pair_id']),
                    marketId: new Uint256(value['market_id']),
                    delegatorId: new Uint256(value['delegator_id']),
                    delegatorAccount: value['delegator_account_hash'] ? Signing.encodeAddress(new Pubkeyhash(new Uint8Array(value['delegator_account_hash']))) || undefined : undefined,
                    marketAccount: value['market_account_hash'] ? Signing.encodeAddress(new Pubkeyhash(new Uint8Array(value['market_account_hash']))) || undefined : undefined,
                    primaryAsset: value['primary_asset'] ? new AssetId(new Uint8Array(value['primary_asset'])).id : undefined,
                    secondaryAsset: value['secondary_asset'] ? new AssetId(new Uint8Array(value['secondary_asset'])).id : undefined,
                    initialValue: new BigNumber(value['initial_value']),
                    currentValue: new BigNumber(value['current_value']),
                    volume: new BigNumber(value['volume']),
                    feeRate: Common.bn(value['fee_rate'])
                });
            }
            return delegatedPools;
        } catch {
            return [];
        }
    }
    static async getPoolsByAccountId(accountId: Uint256, active: boolean | null, cursor: Cursor, connection?: pq.TransactionSql): Promise<Pool[]> {
        const sql = connection || this.connection;
        const result = await this.resultOf(sql`
        SELECT
            pools.*,
            passet.hash AS primary_asset,
            sasset.hash AS secondary_asset,
            accounts.hash AS market_account_hash,
            pools_view.volume
        FROM pools
            INNER JOIN pairs ON pairs.id = pair_id
            INNER JOIN assets passet ON passet.id = pairs.primary_asset_id
            INNER JOIN assets sasset ON sasset.id = pairs.secondary_asset_id
            INNER JOIN markets ON markets.id = market_id
            INNER JOIN accounts ON accounts.id = markets.account_id
            LEFT JOIN pools_view ON pools_view.id = pools.id
        WHERE pools.account_id = ${accountId.toString()}${typeof active == 'boolean' ? sql` AND active = ${active}` : sql``} ORDER BY active DESC, block_number DESC LIMIT ${cursor.count} OFFSET ${cursor.offset}`);
        try {
            const pools = [];
            for (let i = 0; i < result.length; i++) {
                pools.push(this.toPool(result[i]));
            }
            return pools;
        } catch {
            return [];
        }
    }
    static async getPoolsByAccountIdAndMarketPair(accountId: Uint256, marketId: Uint256, pairId: Uint256, active: boolean | null, cursor: Cursor, connection?: pq.TransactionSql): Promise<Pool[]> {
        const sql = connection || this.connection;
        const result = await this.resultOf(sql`
        SELECT
            pools.*,
            passet.hash AS primary_asset,
            sasset.hash AS secondary_asset,
            accounts.hash AS market_account_hash,
            pools_view.volume
        FROM pools
            INNER JOIN pairs ON pairs.id = pair_id
            INNER JOIN assets passet ON passet.id = pairs.primary_asset_id
            INNER JOIN assets sasset ON sasset.id = pairs.secondary_asset_id
            INNER JOIN markets ON markets.id = market_id
            INNER JOIN accounts ON accounts.id = markets.account_id
            LEFT JOIN pools_view ON pools_view.id = pools.id
        WHERE pools.account_id = ${accountId.toString()} AND pools.market_id = ${marketId.toString()} AND pools.pair_id = ${pairId.toString()}${typeof active == 'boolean' ? sql` AND active = ${active}` : sql``} ORDER BY active DESC, block_number DESC LIMIT ${cursor.count} OFFSET ${cursor.offset}`);
        try {
            const pools = [];
            for (let i = 0; i < result.length; i++) {
                pools.push(this.toPool(result[i]));
            }
            return pools;
        } catch {
            return [];
        }
    }
    static async getDelegatorByAccountId(accountId: Uint256, connection?: pq.TransactionSql): Promise<Delegator | null> {
        const sql = connection || this.connection;
        const result = await this.resultOf(sql`
        SELECT
            delegators.*,
            accounts.hash AS account_hash,
            deployer_accounts.hash AS deployer_account_hash
        FROM delegators
            INNER JOIN accounts ON accounts.id = delegators.account_id
            INNER JOIN accounts deployer_accounts ON deployer_accounts.id = delegators.deployer_account_id
        WHERE account_id = ${accountId.toString()}`);
        try {
            return this.toDelegator(result[0]);
        } catch {
            return null;
        }
    }
    static async setDelegator(delegator: Omit<Delegator, 'id'>, connection?: pq.TransactionSql): Promise<Delegator | null> {
        const sql = connection || this.connection;
        const result = await this.resultOf(sql`
        INSERT INTO delegators
        (
            market_id,
            account_id,
            deployer_account_id,
            block_number,
            reward_emission,
            reward_balance,
            permissions
        )
        VALUES
        (
            ${delegator.marketId.toString()},
            ${delegator.accountId.toString()},
            ${delegator.deployerAccountId.toString()},
            ${delegator.blockNumber},
            ${delegator.rewardEmission.toString()},
            ${delegator.rewardBalance.toString()},
            ${sql.json(delegator.permissions.map((x) => ({
                primaryAssetId: x.primaryAssetId.toString(),
                primaryAsset: x.primaryAsset.toString(),
                secondaryAssetId: x.secondaryAssetId.toString(),
                secondaryAsset: x.secondaryAsset.toString()
            })))}::jsonb
        )
        ON CONFLICT (account_id) DO UPDATE SET
            market_id = EXCLUDED.market_id,
            deployer_account_id = EXCLUDED.deployer_account_id,
            block_number = EXCLUDED.block_number,
            reward_emission = EXCLUDED.reward_emission,
            reward_balance = EXCLUDED.reward_balance,
            permissions = EXCLUDED.permissions
        RETURNING *`);

        try {
            return this.toDelegator(result[0]);
        } catch {
            return null;
        }
    }
    static async setDelegatedPool(pool: Omit<DelegatedPool, 'id'>, reset: boolean, connection?: pq.TransactionSql): Promise<DelegatedPool | null> {
        const sql = connection || this.connection;
        const result = await this.resultOf(sql`
        INSERT INTO delegated_pools
        (
            pair_id,
            market_id,
            delegator_id,
            account_id,
            block_number,
            initial_primary_value,
            initial_secondary_value,
            primary_value,
            secondary_value,
            reward_value,
            active
        )
        VALUES
        (
            ${pool.pairId.toString()},
            ${pool.marketId.toString()},
            ${pool.delegatorId.toString()},
            ${pool.accountId.toString()},
            ${pool.blockNumber},
            ${pool.initialPrimaryValue.toString()},
            ${pool.initialSecondaryValue.toString()},
            ${pool.primaryValue.toString()},
            ${pool.secondaryValue.toString()},
            ${pool.rewardValue.toString()},
            ${pool.active}
        )
        ON CONFLICT (pair_id, market_id, delegator_id, account_id) DO UPDATE SET
            block_number = EXCLUDED.block_number,
            initial_primary_value = ${reset ? sql`EXCLUDED` : sql`delegated_pools`}.initial_primary_value,
            initial_secondary_value = ${reset ? sql`EXCLUDED` : sql`delegated_pools`}.initial_secondary_value,
            primary_value = EXCLUDED.primary_value,
            secondary_value = EXCLUDED.secondary_value,
            reward_value = EXCLUDED.reward_value,
            active = EXCLUDED.active
        RETURNING *`);
        try {
            return this.toDelegatedPool(result[0]);
        } catch {
            return null;
        }
    }
    static async getDelegatedPoolById(id: Uint256, connection?: pq.TransactionSql): Promise<DelegatedPool | null> {
        const sql = connection || this.connection;
        const result = await this.resultOf(sql`
        SELECT
            delegated_pools.*,
            passet.hash AS primary_asset,
            sasset.hash AS secondary_asset,
            market_account.hash AS market_account_hash,
            delegator_account.hash AS delegator_account_hash,
            delegators_view.volume,
            (SELECT ARRAY[SUM(v.primary_value), SUM(v.secondary_value)] FROM delegated_pools v WHERE v.pair_id = delegated_pools.pair_id AND v.market_id = delegated_pools.market_id AND v.delegator_id = delegated_pools.delegator_id AND v.active = TRUE) AS virtual_pool,
            (SELECT ARRAY[v.primary_value + v.primary_revenue - v.initial_primary_value, v.secondary_value + v.secondary_revenue - v.initial_secondary_value, v.primary_value, v.secondary_value, v.fee_rate, v.initial_price] FROM pools v WHERE v.pair_id = delegated_pools.pair_id AND v.market_id = delegated_pools.market_id AND v.account_id = delegators.account_id AND v.active = TRUE LIMIT 1) AS shadow_pool
        FROM delegated_pools
            INNER JOIN pairs ON pairs.id = pair_id
            INNER JOIN assets passet ON passet.id = pairs.primary_asset_id
            INNER JOIN assets sasset ON sasset.id = pairs.secondary_asset_id
            INNER JOIN markets ON markets.id = market_id
            INNER JOIN delegators ON delegators.id = delegator_id
            INNER JOIN accounts market_account ON market_account.id = markets.account_id
            INNER JOIN accounts delegator_account ON delegator_account.id = delegators.account_id
            LEFT JOIN delegators_view ON delegators_view.id = delegator_id AND delegators_view.pair_id = pairs.id
        WHERE delegated_pools.id = ${id.toString()}`);
        try {
            return this.toDelegatedPool(result[0]);
        } catch {
            return null;
        }
    }
    static async getDelegatedPoolByHandle(marketId: Uint256, pairId: Uint256, delegatorId: Uint256, accountId: Uint256, connection?: pq.TransactionSql): Promise<DelegatedPool | null> {
        const sql = connection || this.connection;
        const result = await this.resultOf(sql`
        SELECT
            delegated_pools.*,
            passet.hash AS primary_asset,
            sasset.hash AS secondary_asset,
            market_account.hash AS market_account_hash,
            delegator_account.hash AS delegator_account_hash,
            delegators_view.volume,
            (SELECT ARRAY[SUM(v.primary_value), SUM(v.secondary_value)] FROM delegated_pools v WHERE v.pair_id = delegated_pools.pair_id AND v.market_id = delegated_pools.market_id AND v.delegator_id = delegated_pools.delegator_id AND v.active = TRUE) AS virtual_pool,
            (SELECT ARRAY[v.primary_value + v.primary_revenue - v.initial_primary_value, v.secondary_value + v.secondary_revenue - v.initial_secondary_value, v.primary_value, v.secondary_value, v.fee_rate, v.initial_price] FROM pools v WHERE v.pair_id = delegated_pools.pair_id AND v.market_id = delegated_pools.market_id AND v.account_id = delegators.account_id AND v.active = TRUE LIMIT 1) AS shadow_pool
        FROM delegated_pools
            INNER JOIN pairs ON pairs.id = pair_id
            INNER JOIN assets passet ON passet.id = pairs.primary_asset_id
            INNER JOIN assets sasset ON sasset.id = pairs.secondary_asset_id
            INNER JOIN markets ON markets.id = market_id
            INNER JOIN delegators ON delegators.id = delegator_id
            INNER JOIN accounts market_account ON market_account.id = markets.account_id
            INNER JOIN accounts delegator_account ON delegator_account.id = delegators.account_id
            LEFT JOIN delegators_view ON delegators_view.id = delegator_id AND delegators_view.pair_id = pairs.id
        WHERE delegated_pools.market_id = ${marketId.toString()} AND delegated_pools.pair_id = ${pairId.toString()} AND delegated_pools.delegator_id = ${delegatorId.toString()} AND delegated_pools.account_id = ${accountId.toString()}`);
        try {
            return this.toDelegatedPool(result[0]);
        } catch {
            return null;
        }
    }
    static async getAllDelegatedPoolsByDelegatorIdAndMarketPair(delegatorId: Uint256, marketId: Uint256, pairId: Uint256, connection?: pq.TransactionSql): Promise<DelegatedPool[]> {
        const sql = connection || this.connection;
        const result = await this.resultOf(sql`SELECT * FROM delegated_pools WHERE delegator_id = ${delegatorId.toString()} AND market_id = ${marketId.toString()} AND pair_id = ${pairId.toString()}`);
        try {
            const delegatedPools = [];
            for (let i = 0; i < result.length; i++) {
                delegatedPools.push(this.toDelegatedPool(result[i]));
            }
            return delegatedPools;
        } catch {
            return [];
        }
    }
    static async getDelegatedPoolsByAccountIdAndMarketPair(accountId: Uint256, marketId: Uint256, pairId: Uint256, active: boolean | null, cursor: Cursor, connection?: pq.TransactionSql): Promise<DelegatedPool[]> {
        const sql = connection || this.connection;
        const result = await this.resultOf(sql`
        SELECT
            delegated_pools.*,
            passet.hash AS primary_asset,
            sasset.hash AS secondary_asset,
            market_account.hash AS market_account_hash,
            delegator_account.hash AS delegator_account_hash,
            delegators_view.volume,
            (SELECT ARRAY[SUM(v.primary_value), SUM(v.secondary_value)] FROM delegated_pools v WHERE v.pair_id = delegated_pools.pair_id AND v.market_id = delegated_pools.market_id AND v.delegator_id = delegated_pools.delegator_id AND v.active = TRUE) AS virtual_pool,
            (SELECT ARRAY[v.primary_value + v.primary_revenue - v.initial_primary_value, v.secondary_value + v.secondary_revenue - v.initial_secondary_value, v.primary_value, v.secondary_value, v.fee_rate, v.initial_price] FROM pools v WHERE v.pair_id = delegated_pools.pair_id AND v.market_id = delegated_pools.market_id AND v.account_id = delegators.account_id AND v.active = TRUE LIMIT 1) AS shadow_pool
        FROM delegated_pools
            INNER JOIN pairs ON pairs.id = pair_id
            INNER JOIN assets passet ON passet.id = pairs.primary_asset_id
            INNER JOIN assets sasset ON sasset.id = pairs.secondary_asset_id
            INNER JOIN markets ON markets.id = market_id
            INNER JOIN delegators ON delegators.id = delegator_id
            INNER JOIN accounts market_account ON market_account.id = markets.account_id
            INNER JOIN accounts delegator_account ON delegator_account.id = delegators.account_id
            LEFT JOIN delegators_view ON delegators_view.id = delegator_id AND delegators_view.pair_id = pairs.id
        WHERE delegated_pools.account_id = ${accountId.toString()} AND delegated_pools.market_id = ${marketId.toString()} AND delegated_pools.pair_id = ${pairId.toString()}${typeof active == 'boolean' ? sql` AND active = ${active}` : sql``} ORDER BY active DESC, delegated_pools.block_number DESC LIMIT ${cursor.count} OFFSET ${cursor.offset}`);
        try {
            const delegatedPools = [];
            for (let i = 0; i < result.length; i++) {
                delegatedPools.push(this.toDelegatedPool(result[i]));
            }
            return delegatedPools;
        } catch {
            return [];
        }
    }
    static async getDelegatedPoolsByAccountId(accountId: Uint256, active: boolean | null, cursor: Cursor, connection?: pq.TransactionSql): Promise<DelegatedPool[]> {
        const sql = connection || this.connection;
        const result = await this.resultOf(sql`
        SELECT
            delegated_pools.*,
            passet.hash AS primary_asset,
            sasset.hash AS secondary_asset,
            market_account.hash AS market_account_hash,
            delegator_account.hash AS delegator_account_hash,
            delegators_view.volume,
            (SELECT ARRAY[SUM(v.primary_value), SUM(v.secondary_value)] FROM delegated_pools v WHERE v.pair_id = delegated_pools.pair_id AND v.market_id = delegated_pools.market_id AND v.delegator_id = delegated_pools.delegator_id AND v.active = TRUE) AS virtual_pool,
            (SELECT ARRAY[v.primary_value + v.primary_revenue - v.initial_primary_value, v.secondary_value + v.secondary_revenue - v.initial_secondary_value, v.primary_value, v.secondary_value, v.fee_rate, v.initial_price] FROM pools v WHERE v.pair_id = delegated_pools.pair_id AND v.market_id = delegated_pools.market_id AND v.account_id = delegators.account_id AND v.active = TRUE LIMIT 1) AS shadow_pool
        FROM delegated_pools
            INNER JOIN pairs ON pairs.id = pair_id
            INNER JOIN assets passet ON passet.id = pairs.primary_asset_id
            INNER JOIN assets sasset ON sasset.id = pairs.secondary_asset_id
            INNER JOIN markets ON markets.id = market_id
            INNER JOIN delegators ON delegators.id = delegator_id
            INNER JOIN accounts market_account ON market_account.id = markets.account_id
            INNER JOIN accounts delegator_account ON delegator_account.id = delegators.account_id
            LEFT JOIN delegators_view ON delegators_view.id = delegator_id AND delegators_view.pair_id = pairs.id
        WHERE delegated_pools.account_id = ${accountId.toString()}${typeof active == 'boolean' ? sql` AND active = ${active}` : sql``} ORDER BY active DESC, delegated_pools.block_number DESC LIMIT ${cursor.count} OFFSET ${cursor.offset}`);
        try {
            const delegatedPools = [];
            for (let i = 0; i < result.length; i++) {
                delegatedPools.push(this.toDelegatedPool(result[i]));
            }
            return delegatedPools;
        } catch {
            return [];
        }
    }
    static async getDelegationOf(accountId: Uint256, connection?: pq.TransactionSql): Promise<PseudoDelegatedState[]> {
        const sql = connection || this.connection;
        const delegators = await this.resultOf(sql`SELECT
            delegators.id,
            account_id,
            accounts.hash AS delegator_account_hash,
            permissions
        FROM delegators
            INNER JOIN accounts ON accounts.id = delegators.account_id
        WHERE deployer_account_id = ${accountId.toString()}`);
        if (!delegators.length)
            return [];

        let results: PseudoDelegatedState[] = [];
        for (let i = 0; i < delegators.length; i++) {
            const delegator = delegators[i];
            const delegatorAccount: string = Signing.encodeAddress(new Pubkeyhash(delegator['delegator_account_hash'])) || '';
            const delegations = await this.resultOf(sql`
            WITH sources AS (
                SELECT
                    pair_id,
                    SUM(primary_value) AS primary_value,
                    SUM(secondary_value) AS secondary_value
                FROM delegated_pools       
                WHERE delegator_id = ${delegator['id']} AND active = TRUE GROUP BY pair_id
            )
            SELECT
                sources.primary_value,
                sources.secondary_value,
                passet.hash AS primary_asset,
                sasset.hash AS secondary_asset
            FROM sources
                INNER JOIN pairs ON pairs.id = pair_id
                INNER JOIN assets passet ON passet.id = pairs.primary_asset_id
                INNER JOIN assets sasset ON sasset.id = pairs.secondary_asset_id`);
            const delegatedPools = await this.resultOf(sql`
            SELECT
                pools.id,
                price,
                passet.hash AS primary_asset,
                sasset.hash AS secondary_asset,
                primary_value + primary_revenue - initial_primary_value AS primary_delta,
                secondary_value + secondary_revenue - initial_secondary_value AS secondary_delta
            FROM pools
                INNER JOIN pairs ON pairs.id = pair_id
                INNER JOIN assets passet ON passet.id = pairs.primary_asset_id
                INNER JOIN assets sasset ON sasset.id = pairs.secondary_asset_id
            WHERE account_id = ${delegator['account_id']} AND active = TRUE`);
            results = [...results, ...(Array.isArray(delegator['permissions']) ? delegator['permissions'].map((x) => {
                const delegation = delegations.find((v) => new AssetId(v['primary_asset']).id == new AssetId(x.primaryAsset).id && new AssetId(v['secondary_asset']).id == new AssetId(x.secondaryAsset).id) || { };
                const delegatedPool = delegatedPools.find((v) => new AssetId(v['primary_asset']).id == new AssetId(x.primaryAsset).id && new AssetId(v['secondary_asset']).id == new AssetId(x.secondaryAsset).id) || { };
                return {
                    primaryAsset: x.primaryAsset,
                    secondaryAsset: x.secondaryAsset,
                    delegatorAccount: delegatorAccount,
                    poolId: Common.u256(delegatedPool['id']) || new Uint256(0),
                    primaryLiquidity: (Common.bn(delegation['primary_value']) || new BigNumber(0)).plus(Common.bn(delegatedPool['primary_delta']) || new BigNumber(0)),
                    secondaryLiquidity: (Common.bn(delegation['secondary_value']) || new BigNumber(0)).plus(Common.bn(delegatedPool['secondary_delta']) || new BigNumber(0)),
                    price: Common.bn(delegatedPool['price']) || new BigNumber(0)
                };
            }) : [])];
        }
        return results;
    }
    static async setDepth(depth: Omit<Depth, 'id'>, connection?: pq.TransactionSql): Promise<Depth | null> {
        const sql = connection || this.connection;
        const result = await this.resultOf(sql`
        INSERT INTO depths
        (
            pair_id,
            market_id,
            pool_id,
            account_id,
            block_number,
            price,
            quantity,
            time
        )
        VALUES
        (
            ${depth.pairId.toString()},
            ${depth.marketId.toString()},
            ${depth.poolId.toString()},
            ${depth.accountId.toString()},
            ${depth.blockNumber},
            ${depth.price.toString()},
            ${depth.quantity.toString()},
            ${depth.time.getTime()}
        )
        RETURNING *`);
        try {
            return this.toDepth(result[0]);
        } catch {
            return null;
        }
    }
    static async setTrade(trade: Omit<Trade, 'id'>, connection?: pq.TransactionSql): Promise<Trade | null> {
        const sql = connection || this.connection;
        const result = await this.resultOf(sql`
        INSERT INTO trades
        (
            pair_id,
            market_id,
            maker_order_id,
            maker_pool_id,
            maker_account_id,
            taker_order_id,
            taker_account_id,
            block_number,
            side,
            price,
            quantity,
            time
        )
        VALUES
        (
            ${trade.pairId.toString()},
            ${trade.marketId ? trade.marketId.toString() : null},
            ${trade.makerOrderId ? trade.makerOrderId.toString() : null},
            ${trade.makerPoolId ? trade.makerPoolId.toString() : null},
            ${trade.makerAccountId ? trade.makerAccountId.toString() : null},
            ${trade.takerOrderId ? trade.takerOrderId.toString() : null},
            ${trade.takerAccountId ? trade.takerAccountId.toString() : null},
            ${trade.blockNumber ? trade.blockNumber : null},
            ${trade.side},
            ${trade.price.toString()},
            ${trade.quantity.toString()},
            ${trade.time.getTime()}
        )
        RETURNING *`);
        try {
            return this.toTrade(result[0]);
        } catch {
            return null;
        }
    }
    static async cleanupLogs(marketId: Uint256, blockNumber: number, connection?: pq.TransactionSql): Promise<void> {
        const sql = connection || this.connection;
        try {
            await this.resultOf(sql`DELETE FROM depths WHERE market_id = ${marketId.toString()} AND block_number = ${blockNumber}`);
            await this.resultOf(sql`DELETE FROM trades WHERE market_id = ${marketId.toString()} AND block_number = ${blockNumber}`);
        } catch { }
    }
    static async getAssetPriceHistory(assets: AssetId[], interval: number, points: number, connection?: pq.TransactionSql): Promise<Record<string, AssetId & { history: [number, BigNumber][] }>> {
        const sql = connection || this.connection;
        const ids: (Uint256 | null)[] = (await Promise.all(assets.map(v => this.getAssetIdByHash(v, 'read-only', connection))));
        const mapping: Record<string, AssetId> = { };
        for (let i = 0; i < ids.length; i++) {
            const id = ids[i];
            if (id != null) {
                mapping[id.toString()] = assets[i];
            }
        }

        const results = await this.resultOf(sql`
        WITH targets AS (
            SELECT
                assets.id AS asset_id,
                pairs.id AS pair_id
            FROM assets
                INNER JOIN pairs ON primary_asset_id = assets.id AND secondary_asset_id IS NULL
            WHERE assets.id = ANY(${ids.filter(v => v != null).map(v => v.toString())})
        ), points AS (
            SELECT
                (SELECT EXTRACT(EPOCH FROM CURRENT_DATE::TIMESTAMP)::BIGINT - (${interval / 1000})::BIGINT * (${points} - step)::BIGINT) * 1000 AS min_time,
                (SELECT EXTRACT(EPOCH FROM CURRENT_DATE::TIMESTAMP)::BIGINT - (${interval / 1000})::BIGINT * (${points - 1} - step)::BIGINT) * 1000 AS max_time
            FROM GENERATE_SERIES(0, ${points - 1}) step
        )
        SELECT
            asset_id,
            max_time - 1 AS time,
            (SELECT price FROM trades WHERE pair_id = targets.pair_id AND time BETWEEN min_time AND max_time ORDER BY time DESC LIMIT 1)
        FROM targets
            INNER JOIN points ON TRUE
        ORDER BY min_time ASC`);
        const history: Record<string, AssetId & { history: [number, BigNumber][] }> = { };
        for (let i = 0; i < results.length; i++) {
            const result = results[i];
            const asset = mapping[result['asset_id'].toString()];
            const time = Common.num(result['time']);
            if (asset != null && time != null) {
                const bucket = history[asset.id];
                const lastPrice = bucket && bucket.history.length > 0 ? bucket.history[bucket.history.length - 1][1] : new BigNumber(0);
                const price = Common.bn(result['price']) || lastPrice || new BigNumber(0);
                if (!bucket) {
                    const pseudoAsset: AssetId & { history: [number, BigNumber][] } = new AssetId(asset.id) as any;
                    pseudoAsset.history = [[time, price]];
                    history[asset.id] = pseudoAsset;
                } else {
                    bucket.history.push([time, price]);
                }
            }
        }
        return history;
    }
    static async getAggregatedTradesByPairId(pairId: Uint256, cursor: TimeCursor, connection?: pq.TransactionSql): Promise<AggregatedTrade[]> {
        const sql = connection || this.connection;
        const bindings = await this.resultOf(sql`
        SELECT
            ppair.id AS secondary_id,
            spair.id AS tertiary_id
        FROM pairs
            INNER JOIN pairs ppair ON ppair.primary_asset_id = pairs.primary_asset_id AND ppair.secondary_asset_id IS NULL
            INNER JOIN pairs spair ON spair.primary_asset_id = pairs.secondary_asset_id AND spair.secondary_asset_id IS NULL
        WHERE pairs.id = ${pairId.toString()}`);
        
        const pairings = {
            primary: pairId.toInteger(),
            secondary: bindings.length > 0 ? parseInt(bindings[0]['secondary_id']) : null,
            tertiary: bindings.length > 0 ? parseInt(bindings[0]['tertiary_id']) : null
        };
        const trades = await this.resultOf(sql`
        SELECT
            pair_id,
            time_bucket(${cursor.interval}, time + ${cursor.interval}) - ${cursor.interval} AS timepoint,
            SUM(quantity) AS volume,
            FIRST(price, time) AS open,
            MIN(price) AS low,
            MAX(price) AS high,
            LAST(price, time) AS close
        FROM trades
        WHERE ${pairings.secondary != null && pairings.tertiary != null ? sql`pair_id IN (${pairings.primary}, ${pairings.secondary}, ${pairings.tertiary})` : sql`pair_id = ${pairings.primary}`} AND time BETWEEN ${cursor.fromTime} AND ${cursor.toTime}
        GROUP BY pair_id, timepoint
        ORDER BY timepoint`);
        if (!trades.length)
            return [];

        const primary: AggregatedTrade[] = [], secondary: AggregatedTrade[] = [], tertiary: AggregatedTrade[] = [];
        for (let i = 0; i < trades.length; i++) {
            const trade = trades[i];
            const pairId = parseInt(trade['pair_id']);
            if (pairId == pairings.primary)
                primary.push(this.toAggregatedTrade(trade));
            else if (pairId == pairings.secondary)
                secondary.push(this.toAggregatedTrade(trade));
            else if (pairId == pairings.tertiary)
                tertiary.push(this.toAggregatedTrade(trade));
        }

        for (let i = 0; i < secondary.length; i++) {
            const target = secondary[i];
            const [relative] = this.toBestSeriesItem(tertiary, target.timepoint) as ([AggregatedTrade | null, number]);
            if (relative != null) {
                const [sibling, position] = this.toBestSeriesItem(primary, target.timepoint, true) as ([AggregatedTrade | null, number]);
                const parent = sibling && position >= 1 ? primary[position - 1] : null;
                primary.splice(position, sibling ? 1 : 0, {
                    timepoint: target.timepoint,
                    volume: sibling ? target.volume.dividedBy(relative.close).plus(sibling.volume) : target.volume.dividedBy(relative.close),
                    open: sibling ? (parent ? parent.close : sibling.open) : target.open.dividedBy(relative.close),
                    low: sibling ? BigNumber.min(target.low.dividedBy(relative.close), sibling.low) : target.low.dividedBy(relative.close),
                    high: sibling ? BigNumber.max(target.high.dividedBy(relative.close), sibling.high) : target.high.dividedBy(relative.close),
                    close: sibling ? target.close.dividedBy(relative.close).plus(sibling.close).dividedBy(2) : target.close.dividedBy(relative.close)
                });
            }
        }
        for (let i = 0; i < tertiary.length; i++) {
            const target = tertiary[i];
            const [relative] = this.toBestSeriesItem(secondary, target.timepoint) as ([AggregatedTrade | null, number]);
            if (relative != null) {
                const [sibling, position] = this.toBestSeriesItem(primary, target.timepoint, true) as ([AggregatedTrade | null, number]);
                const parent = sibling && position >= 1 ? primary[position - 1] : null;
                primary.splice(position, sibling ? 1 : 0, {
                    timepoint: target.timepoint,
                    volume: sibling ? target.volume.dividedBy(target.close).plus(sibling.volume) : target.volume.dividedBy(target.close),
                    open: sibling ? (parent ? parent.close : sibling.open) : relative.close.dividedBy(target.open),
                    low: sibling ? BigNumber.min(relative.close.dividedBy(target.low), sibling.low) : relative.close.dividedBy(target.low),
                    high: sibling ? BigNumber.max(relative.close.dividedBy(target.high), sibling.high) : relative.close.dividedBy(target.high),
                    close: sibling ? relative.close.dividedBy(target.close).plus(sibling.close).dividedBy(2) : relative.close.dividedBy(target.close)
                });
            }
        }
        return this.toFixedSeries(primary, cursor.fromTime, cursor.toTime, cursor.interval, (base: AggregatedTrade) => ({
            ...base,
            side: OrderSide.Buy,
            volume: new BigNumber(0),
            open: base.close,
            low: base.close,
            high: base.close,
            close: base.close
        }));
    }
    static async getAggregatedLogsByMarketPair(marketId: Uint256, pairId: Uint256, cursor: Cursor, connection?: pq.TransactionSql): Promise<AggregatedLog[]> {
        const sql = connection || this.connection;
        const result = await this.resultOf(sql`
        (
            SELECT
                accounts.hash AS account_hash,
                time,
                10 AS side,
                price,
                quantity
            FROM depths
                LEFT JOIN accounts ON accounts.id = account_id
            WHERE pair_id = ${pairId.toString()} AND market_id = ${marketId.toString()}
        )
        UNION ALL
        (
            SELECT
                COALESCE(taker_account.hash, maker_account.hash) AS account_hash,
                time,
                side,
                price,
                quantity
            FROM trades
                LEFT JOIN accounts maker_account ON maker_account.id = maker_account_id
                LEFT JOIN accounts taker_account ON taker_account.id = taker_account_id
            WHERE pair_id = ${pairId.toString()} AND market_id = ${marketId.toString()} AND (maker_account.hash IS NOT NULL OR taker_account.hash IS NOT NULL)
        )
        ORDER BY time DESC LIMIT ${cursor.count} OFFSET ${cursor.offset}`);
        try {
            return result.map((item) => {
                const side = Common.num(item['side']);
                return {
                    time: Common.num(item['time']) || new Date().getTime(),
                    account: item['account_hash'] ? Signing.encodeAddress(new Pubkeyhash(new Uint8Array(item['account_hash']))) || '' : '',
                    side: side == 10 ? 'lp' : side as OrderSide,
                    price: Common.bn(item['price']) || new BigNumber(0),
                    quantity: Common.bn(item['quantity']) || new BigNumber(0)
                }
            });
        } catch {
            return [];
        }
    }
    static async getAggregatedLevelsByMarketPair(marketId: Uint256, pairId: Uint256, levels: number, connection?: pq.TransactionSql): Promise<{ ask: AggregatedLevel[], bid: AggregatedLevel[] }> {
        const sql = connection || this.connection;
        const result = await this.resultOf(sql`
        (
            SELECT
                side,
                id,
                last_price AS price,
                last_quantity AS quantity,
                NULL AS min_price,
                NULL AS max_price,
                NULL AS primary_value,
                NULL AS secondary_value,
                NULL AS fee_rate
            FROM orders WHERE market_id = ${marketId.toString()} AND pair_id = ${pairId.toString()} AND side = ${OrderSide.Sell} AND active = TRUE AND last_price > 0 AND last_quantity > 0
            ORDER BY price ASC LIMIT ${levels}
        )
        UNION ALL
        (
            SELECT
                ${OrderSide.Sell} AS side,
                id,
                last_ask_price AS price,
                primary_value AS quantity,
                min_price,
                max_price,
                primary_value,
                secondary_value,
                fee_rate
            FROM pools WHERE market_id = ${marketId.toString()} AND pair_id = ${pairId.toString()} AND active = TRUE AND last_ask_price > 0 AND primary_value > 0 AND (max_price IS NULL OR last_ask_price <= max_price)
            ORDER BY price ASC LIMIT ${levels}
        )
        UNION ALL
        (
            SELECT
                side,
                id,
                last_price AS price,
                last_quantity AS quantity,
                NULL AS min_price,
                NULL AS max_price,
                NULL AS primary_value,
                NULL AS secondary_value,
                NULL AS fee_rate
            FROM orders WHERE market_id = ${marketId.toString()} AND pair_id = ${pairId.toString()} AND side = ${OrderSide.Buy} AND active = TRUE AND last_price > 0 AND last_quantity > 0
            ORDER BY price DESC LIMIT ${levels}
        )
        UNION ALL
        (
            SELECT
                ${OrderSide.Buy} AS side,
                id,
                last_bid_price AS price,
                secondary_value / last_bid_price AS quantity,
                min_price,
                max_price,
                primary_value,
                secondary_value,
                fee_rate
            FROM pools WHERE market_id = ${marketId.toString()} AND pair_id = ${pairId.toString()} AND active = TRUE AND last_bid_price > 0 AND secondary_value > 0 AND (min_price IS NULL OR last_bid_price >= min_price)
            ORDER BY price ASC LIMIT ${levels}
        )`);
        const ask: AggregatedLevel[] = [], bid: AggregatedLevel[] = [];
        for (let i = 0; i < result.length; i++) {
            const value = result[i];
            const target = value['side'] == OrderSide.Sell ? ask : bid;
            target.push(this.toAggregatedLevel(value));
        }
        return {
            ask: ask.sort((a, b) => a.price.minus(b.price).toNumber()).slice(0, levels),
            bid: bid.sort((a, b) => b.price.minus(a.price).toNumber()).slice(0, levels)
        }
    }
    static async getAggregatedPolyAssetIdsByMarketPair(marketId: Uint256, pairId: Uint256, connection?: pq.TransactionSql): Promise<{ primary: AssetId[], secondary: AssetId[] }> {
        const sql = connection || this.connection;
        const result = await this.resultOf(sql`
        WITH poly_ids AS (
            SELECT
                COALESCE((SELECT poly_asset_id FROM poly_assets WHERE asset_id = pairs.primary_asset_id AND market_id = ${marketId.toString()}), (SELECT poly_asset_id FROM poly_assets WHERE poly_asset_id = pairs.primary_asset_id AND market_id = ${marketId.toString()} LIMIT 1)) AS primary_poly_asset_id,
		        COALESCE((SELECT poly_asset_id FROM poly_assets WHERE asset_id = pairs.secondary_asset_id AND market_id = ${marketId.toString()}), (SELECT poly_asset_id FROM poly_assets WHERE poly_asset_id = pairs.secondary_asset_id AND market_id = ${marketId.toString()} LIMIT 1)) AS secondary_poly_asset_id
	        FROM pairs WHERE pairs.id = ${pairId.toString()}
        )
        SELECT asset_id, (poly_asset_id = COALESCE(primary_poly_asset_id, 0)) AS as_primary FROM poly_assets
            INNER JOIN poly_ids ON TRUE
        WHERE market_id = ${marketId.toString()} AND poly_asset_id IN (SELECT unnest(ARRAY[primary_poly_asset_id, secondary_poly_asset_id]) FROM poly_ids)`);
        const primary: AssetId[] = [], secondary: AssetId[] = [];
        for (let i = 0; i < result.length; i++) {
            const value = result[i];
            const target = value['as_primary'] ? primary : secondary;
            const asset = await this.getAssetHashById(Common.u256(value['asset_id']) || new Uint256(), connection);
            if (asset != null)
                target.push(asset);
        }
        return { primary: primary, secondary: secondary };
    }
    static async getAggregatedPolyAssetIdsByMarket(targetAssetId: Uint256, connection?: pq.TransactionSql): Promise<(AssetId & { marketId?: Uint256 })[]> {
        const sql = connection || this.connection;
        const polyResult = await this.resultOf(sql`SELECT poly_asset_id, market_id FROM poly_assets WHERE asset_id = ${targetAssetId.toString()} OR poly_asset_id = ${targetAssetId.toString()} LIMIT 1`);
        const polyAssetId = (polyResult.length > 0 ? Common.u256(polyResult[0]['poly_asset_id']) : targetAssetId) || targetAssetId;
        const result = await this.resultOf(sql`SELECT asset_id, market_id FROM poly_assets WHERE poly_asset_id = ${polyAssetId.toString()}`);
        const assets: (AssetId & { marketId?: Uint256 })[] = [];
        for (let i = 0; i < result.length; i++) {
            const assetId = Common.u256(result[i]['asset_id']) || new Uint256();
            const marketId = Common.u256(result[i]['market_id']) || new Uint256();
            const asset: (AssetId & { marketId?: Uint256 }) | null = await this.getAssetHashById(assetId, connection);
            if (asset != null) {
                asset.marketId = marketId;
                assets.push(asset);
            }
        }
        
        const polyAsset: (AssetId & { marketId?: Uint256 }) | null = await this.getAssetHashById(polyAssetId, connection);
        if (polyAsset != null && result.length > 0) {
            polyAsset.marketId = Common.u256(result[0]['market_id']) || new Uint256();
            assets.push(polyAsset);
        }
        return assets;
    }
    static async getRoutingAmountByPairId(marketId: Uint256, pairId: Uint256, side: OrderSide, amount: [BigNumber, BigNumber], slippage: BigNumber, connection?: pq.TransactionSql): Promise<[BigNumber, BigNumber]> {
        const sql = connection || this.connection;
        const result = await this.resultOf(side == OrderSide.Buy ? sql`
        WITH levels AS (
            (
                SELECT
                    last_price AS price,
                    last_quantity AS quantity
                FROM orders WHERE market_id = ${marketId.toString()} AND pair_id = ${pairId.toString()} AND side = ${OrderSide.Sell} AND active = TRUE AND last_price > 0 AND last_quantity > 0
            )
            UNION ALL
            (
                SELECT
                    last_ask_price AS price,
                    primary_value AS quantity
                FROM pools WHERE market_id = ${marketId.toString()} AND pair_id = ${pairId.toString()} AND active = TRUE AND last_ask_price > 0 AND primary_value > 0 AND (max_price IS NULL OR last_ask_price <= max_price)
            )
        ) SELECT price, SUM(quantity) AS quantity FROM levels GROUP BY price ORDER BY price ASC` : sql`
        WITH levels AS (
            (
                SELECT
                    last_price AS price,
                    last_quantity AS quantity
                FROM orders WHERE market_id = ${marketId.toString()} AND pair_id = ${pairId.toString()} AND side = ${OrderSide.Buy} AND active = TRUE AND last_price > 0 AND last_quantity > 0
                ORDER BY price DESC
            )
            UNION ALL
            (
                SELECT
                    last_bid_price AS price,
                    secondary_value / last_bid_price AS quantity
                FROM pools WHERE market_id = ${marketId.toString()} AND pair_id = ${pairId.toString()} AND active = TRUE AND last_bid_price > 0 AND secondary_value > 0 AND (min_price IS NULL OR last_bid_price >= min_price)
                ORDER BY price ASC
            )
        ) SELECT price, SUM(quantity) AS quantity FROM levels GROUP BY price ORDER BY price DESC`);
        const levels: { price: BigNumber, quantity: BigNumber }[] = result.map((x) => ({ price: Common.bn(x['price']) || new BigNumber(0), quantity: Common.bn(x['quantity']) || new BigNumber(0) }));
        const fee = new BigNumber(1).minus(slippage);
        let minAmountIn = new BigNumber(amount[0]);
        let maxAmountIn = new BigNumber(amount[1]);
        let minAmountOut = new BigNumber(0);
        let maxAmountOut = new BigNumber(0);
        while (levels.length > 0 && (minAmountIn.gt(0) || maxAmountIn.gt(0))) {
            const level = levels.shift()!;
            if (side == OrderSide.Sell) {
                if (minAmountIn.gt(0)) {
                    const quantity = BigNumber.min(minAmountIn, level.quantity);
                    minAmountIn = minAmountIn.minus(quantity);
                    minAmountOut = minAmountOut.plus(quantity.multipliedBy(level.price)).multipliedBy(fee);
                }
                if (maxAmountIn.gt(0)) {
                    const quantity = BigNumber.min(maxAmountIn, level.quantity);
                    maxAmountIn = maxAmountIn.minus(quantity);
                    maxAmountOut = maxAmountOut.plus(quantity.multipliedBy(level.price));
                }
            } else {
                if (minAmountIn.gt(0)) {
                    const quantity = BigNumber.min(minAmountIn, level.price.multipliedBy(level.quantity));
                    minAmountIn = minAmountIn.minus(quantity);
                    minAmountOut = minAmountOut.plus(quantity.dividedBy(level.price)).multipliedBy(fee);
                }
                if (maxAmountIn.gt(0)) {
                    const quantity = BigNumber.min(maxAmountIn, level.price.multipliedBy(level.quantity));
                    maxAmountIn = maxAmountIn.minus(quantity);
                    maxAmountOut = maxAmountOut.plus(quantity.dividedBy(level.price));
                }
            }
        }
        return [minAmountOut, maxAmountOut];
    }
    static async getRoutingPathsByAssetIds(marketId: Uint256, fromAssetId: Uint256, toAssetId: Uint256, amount: BigNumber, slippage: BigNumber, depth: number, connection?: pq.TransactionSql): Promise<RouterPath[]> {
        const allPaths: RouterPath[] = [];
        const fromPolyAsset = (await this.getPolyAssetId(fromAssetId, marketId)) || fromAssetId;
        const toPolyAsset = (await this.getPolyAssetId(toAssetId, marketId)) || toAssetId;
        const queue: [Uint256, [BigNumber, BigNumber], Set<string>, RouterPath][] = [[fromPolyAsset, [amount, amount], new Set([fromPolyAsset.toCompactHex()]), []]];
        while (queue.length > 0) {
            const [assetIn, amountIn, set, path] = queue.shift()!;
            if (assetIn.eq(toPolyAsset)) {
                allPaths.push(path);
                continue;
            } else if (path.length >= depth) {
                continue;
            }

            const closestPairs = await this.getClosestPairsById(assetIn, connection);
            for (const pair of closestPairs) {
                const isSelling = pair.primaryAsset ? pair.primaryAsset.id.eq(assetIn) : false;
                const assetOut = isSelling ? pair.secondaryAsset : pair.primaryAsset;
                if (!assetOut || set.has(assetOut.id.toCompactHex()))
                    continue;

                const side = isSelling ? OrderSide.Sell : OrderSide.Buy;
                const amountOut = await this.getRoutingAmountByPairId(marketId, pair.id, side, amountIn, slippage, connection);
                if (amountOut[0].gt(0) || amountOut[1].gt(0)) {
                    queue.push([
                        assetOut.id,
                        amountOut,
                        new Set(set).add(assetOut.id.toCompactHex()),
                        [...path, { pair, side: side, input: { min: amountIn[0], max: amountIn[1] }, output: { min: amountOut[0], max: amountOut[1] } }],
                    ]);
                }
            }
        }
        return allPaths.sort((a, b) => b[b.length - 1].output.min.comparedTo(a[a.length - 1].output.min) || 0);
    }
    private static toBlock(value: pq.Row): Block {
        return {
            blockNumber: Common.num(value['block_number']) || 0,
            blockHash: Common.u256(value['block_hash']) || new Uint256(),
        }
    }
    private static toMarket(value: pq.Row): Market {
        const account = value['account_hash'] ? Signing.encodeAddress(new Pubkeyhash(new Uint8Array(value['account_hash']))) || undefined : undefined;
        return {
            id: new Uint256(value['id']),
            accountId: new Uint256(value['account_id']),
            account: account,
            version: account ? Blockchain.accountOf(account)?.version || undefined : undefined,
            deployerAccountId: new Uint256(value['deployer_account_id']),
            deployerAccount: value['deployer_account_hash'] ? Signing.encodeAddress(new Pubkeyhash(new Uint8Array(value['deployer_account_hash']))) || undefined : undefined,
            blockNumber: Common.num(value['block_number']) || 0,
            poolExitFee: Common.bn(value['pool_exit_fee']) || new BigNumber(0),
            maxPoolFeeRate: Common.bn(value['max_pool_fee_rate']) || new BigNumber(0),
            minMakerFee: Common.bn(value['min_maker_fee']) || new BigNumber(0),
            maxMakerFee: Common.bn(value['max_maker_fee']) || new BigNumber(0),
            makerFeeExponent: Common.num(value['maker_fee_exponent']) || 0,
            minTakerFee: Common.bn(value['min_taker_fee']) || new BigNumber(0),
            maxTakerFee: Common.bn(value['max_taker_fee']) || new BigNumber(0),
            takerFeeExponent: Common.num(value['taker_fee_exponent']) || 0,
            assetVolumeTarget: Common.bn(value['asset_volume_target']) || new BigNumber(0),
            assetResetDays: Common.num(value['asset_reset_days']) || 0,
            accountResetDays: Common.num(value['account_reset_days']) || 0,
            marketPolicy: value['market_policy'] as MarketPolicy
        }
    }
    private static toOrder(value: pq.Row): Order {
        return {
            id: new Uint256(value['id']),
            orderId: new Uint256(value['order_id']),
            pairId: new Uint256(value['pair_id']),
            primaryAsset: value['primary_asset'] ? new AssetId(new Uint8Array(value['primary_asset'])).id : undefined,
            secondaryAsset: value['secondary_asset'] ? new AssetId(new Uint8Array(value['secondary_asset'])).id : undefined,
            marketId: new Uint256(value['market_id']),
            marketAccount: value['market_account_hash'] ? Signing.encodeAddress(new Pubkeyhash(new Uint8Array(value['market_account_hash']))) || undefined : undefined,
            accountId: new Uint256(value['account_id']),
            blockNumber: value['block_number'],
            condition: value['condition'] as OrderCondition,
            side: value['side'] as OrderSide,
            policy: value['policy'] as OrderPolicy,
            price: Common.bn(value['price']),
            stopPrice: Common.bn(value['stop_price']),
            fillingPrice: Common.bn(value['filling_price']),
            startingValue: new BigNumber(value['starting_value']),
            value: new BigNumber(value['value']),
            slippage: Common.bn(value['slippage']),
            trailingStep: Common.bn(value['trailing_step']),
            trailingDistance: Common.bn(value['trailing_distance']),
            lastPrice: Common.bn(value['last_price']) || new BigNumber(0),
            lastQuantity: Common.bn(value['last_quantity']) || new BigNumber(0),
            active: value['active']
        };
    }
    private static toPool(value: pq.Row): Pool {
        return {
            id: new Uint256(value['id']),
            poolId: new Uint256(value['pool_id']),
            pairId: new Uint256(value['pair_id']),
            primaryAsset: value['primary_asset'] ? new AssetId(new Uint8Array(value['primary_asset'])).id : undefined,
            secondaryAsset: value['secondary_asset'] ? new AssetId(new Uint8Array(value['secondary_asset'])).id : undefined,
            marketId: new Uint256(value['market_id']),
            marketAccount: value['market_account_hash'] ? Signing.encodeAddress(new Pubkeyhash(new Uint8Array(value['market_account_hash']))) || undefined : undefined,
            accountId: new Uint256(value['account_id']),
            blockNumber: value['block_number'],
            initialPrice: new BigNumber(value['initial_price']),
            initialPrimaryValue: new BigNumber(value['initial_primary_value']),
            initialSecondaryValue: new BigNumber(value['initial_secondary_value']),
            primaryValue: new BigNumber(value['primary_value']),
            secondaryValue: new BigNumber(value['secondary_value']),
            primaryRevenue: new BigNumber(value['primary_revenue']),
            secondaryRevenue: new BigNumber(value['secondary_revenue']),
            liquidity: new BigNumber(value['liquidity']),
            price: new BigNumber(value['price']),
            minPrice: new BigNumber(value['min_price']),
            maxPrice: new BigNumber(value['max_price']),
            feeRate: new BigNumber(value['fee_rate']),
            exitFee: new BigNumber(value['exit_fee']),
            lastAskPrice: Common.bn(value['last_ask_price']) || new BigNumber(0),
            lastBidPrice: Common.bn(value['last_bid_price']) || new BigNumber(0),
            volume: Common.bn(value['volume']),
            active: value['active']
        };
    }
    private static toTrade(value: pq.Row): Trade {
        return {
            pairId: new Uint256(value['pair_id']),
            marketId: Common.u256(value['market_id']),
            makerOrderId: Common.u256(value['maker_order_id']),
            makerPoolId: Common.u256(value['maker_pool_id']),
            makerAccountId: Common.u256(value['maker_account_id']),
            takerOrderId: Common.u256(value['taker_order_id']),
            takerAccountId: Common.u256(value['taker_account_id']),
            blockNumber: value['block_number'] || null,
            side: value['side'] as OrderSide,
            price: new BigNumber(value['price']),
            quantity: new BigNumber(value['quantity']),
            time: new Date(value['time'])
        };
    }
    private static toDepth(value: pq.Row): Depth {
        return {
            pairId: new Uint256(value['pair_id']),
            marketId: Common.u256(value['market_id']) || new Uint256(0),
            poolId: Common.u256(value['pool_id']) || new Uint256(0),
            accountId: Common.u256(value['account_id']) || new Uint256(0),
            blockNumber: value['block_number'] || 0,
            price: new BigNumber(value['price']),
            quantity: new BigNumber(value['quantity']),
            time: new Date(value['time'])
        };
    }
    private static toDelegator(value: pq.Row): Delegator {
        const permissions = value['permissions'];
        return {
            id: new Uint256(value['id']),
            marketId: Common.u256(value['market_id']) || new Uint256(0),
            accountId: Common.u256(value['account_id']) || new Uint256(0),
            account: value['account_hash'] ? Signing.encodeAddress(new Pubkeyhash(new Uint8Array(value['account_hash']))) || undefined : undefined,
            deployerAccountId: Common.u256(value['deployer_account_id']) || new Uint256(0),
            deployerAccount: value['deployer_account_hash'] ? Signing.encodeAddress(new Pubkeyhash(new Uint8Array(value['deployer_account_hash']))) || undefined : undefined,
            blockNumber: value['block_number'] || 0,
            rewardEmission: new BigNumber(value['reward_emission']),
            rewardBalance: new BigNumber(value['reward_balance']),
            permissions: Array.isArray(permissions) ? permissions.map((x) => ({
                primaryAssetId: Common.u256(x.primaryAssetId) || new Uint256(0),
                primaryAsset: x.primaryAsset,
                secondaryAssetId: Common.u256(x.secondaryAssetId) || new Uint256(0),
                secondaryAsset: x.secondaryAsset
            })) : []
        };
    }
    private static toDelegatedPool(value: pq.Row): DelegatedPool {
        const virtualPool = value['virtual_pool'] || [], shadowPool = value['shadow_pool'] || [];
        const primaryValue = new BigNumber(value['primary_value']);
        const secondaryValue = new BigNumber(value['secondary_value']);
        const primaryPrevTotal = Common.bn(virtualPool[0]) || new BigNumber(0);
        const secondaryPrevTotal = Common.bn(virtualPool[1]) || new BigNumber(0);
        const primaryDelta = Common.bn(shadowPool[0]) || new BigNumber(0);
        const secondaryDelta = Common.bn(shadowPool[1]) || new BigNumber(0);
        const primaryTotal = primaryPrevTotal.plus(primaryDelta);
        const secondaryTotal = secondaryPrevTotal.plus(secondaryDelta);
        const primaryReserve = Common.bn(shadowPool[2]) || new BigNumber(0);
        const secondaryReserve = Common.bn(shadowPool[3]) || new BigNumber(0);
        const feeRate = Common.bn(shadowPool[4]);
        const price = Common.bn(shadowPool[5]) || new BigNumber(0);
        const total = primaryPrevTotal.multipliedBy(price).plus(secondaryPrevTotal);
        const share = total.gt(0) ? primaryValue.multipliedBy(price).plus(secondaryValue).dividedBy(total) : new BigNumber(0);
        const dynamic = virtualPool.length == 2 && shadowPool.length == 6 && (!primaryDelta.eq(0) || !secondaryDelta.eq(0));
        return {
            id: new Uint256(value['id']),
            delegatorId: new Uint256(value['delegator_id']),
            delegatorAccount: value['delegator_account_hash'] ? Signing.encodeAddress(new Pubkeyhash(new Uint8Array(value['delegator_account_hash']))) || undefined : undefined,
            pairId: new Uint256(value['pair_id']),
            marketId: new Uint256(value['market_id']),
            marketAccount: value['market_account_hash'] ? Signing.encodeAddress(new Pubkeyhash(new Uint8Array(value['market_account_hash']))) || undefined : undefined,
            accountId: new Uint256(value['account_id']),
            primaryAsset: value['primary_asset'] ? new AssetId(new Uint8Array(value['primary_asset'])).id : undefined,
            secondaryAsset: value['secondary_asset'] ? new AssetId(new Uint8Array(value['secondary_asset'])).id : undefined,
            blockNumber: value['block_number'],
            rewardValue: new BigNumber(value['reward_value']),
            initialPrimaryValue: new BigNumber(value['initial_primary_value']),
            initialSecondaryValue: new BigNumber(value['initial_secondary_value']),
            primaryValue: dynamic ? primaryTotal.multipliedBy(share) : primaryValue,
            secondaryValue: dynamic ? secondaryTotal.multipliedBy(share) : secondaryValue,
            primaryTotal: primaryTotal,
            secondaryTotal: secondaryTotal,
            primaryReserve: primaryReserve,
            secondaryReserve: secondaryReserve,
            allocationPrice: price.gt(0) ? price : undefined,
            volume: Common.bn(value['volume']),
            feeRate: feeRate,
            share: share,
            active: value['active']
        };
    }
    private static toAggregatedTrade(value: pq.Row): AggregatedTrade {
        return {
            timepoint: parseInt(value['timepoint']),
            volume: Common.bn(value['volume']) || new BigNumber(0),
            open: Common.bn(value['open']) || new BigNumber(0),
            low: Common.bn(value['low']) || new BigNumber(0),
            high: Common.bn(value['high']) || new BigNumber(0),
            close: Common.bn(value['close']) || new BigNumber(0),
        };
    }
    private static toAggregatedLevel(value: pq.Row): AggregatedLevel {
        const minPrice = Common.bn(value['min_price']);
        const maxPrice = Common.bn(value['max_price']);
        const primaryValue = Common.bn(value['primary_value']);
        const secondaryValue = Common.bn(value['secondary_value']);
        const feeRate = Common.bn(value['fee_rate']);
        return {
            id: Common.u256(value['id']) || new Uint256(0),
            price: Common.bn(value['price']) || new BigNumber(0),
            quantity: Common.bn(value['quantity']) || new BigNumber(0),
            curve: minPrice || maxPrice || primaryValue || secondaryValue || feeRate ? {
                minPrice: minPrice || null,
                maxPrice: maxPrice || null,
                primaryValue: primaryValue || new BigNumber(0),
                secondaryValue: secondaryValue || new BigNumber(0),
                feeRate: feeRate || new BigNumber(0)
            } : undefined
        };
    }
    private static set(type: NodeCache, key: string, value?: string): boolean {
        try {
            return type.set(key, value);
        } catch {
            return false;
        }
    }
    private static get(type: NodeCache, key: string): string | null {
        try {
            return type.get(key) || null;
        } catch {
            return null;
        }
    }
    private static toBestSeriesItem(series: { timepoint: number }[], timepoint: number, exactOnly?: boolean): [any, number] {
        if (!series.length)
            return [null, 0];
        
        let left = 0, right = series.length - 1;
        while (left <= right) {
            const mid = Math.floor((left + right) / 2);
            const item = series[mid];
            if (item.timepoint == timepoint) {
                return [item, mid];
            } else if (item.timepoint < timepoint) {
                left = mid + 1;
            } else {
                right = mid - 1;
            }
        }
        
        if (exactOnly)
            return [null, left];

        const leftCandidate = left < series.length ? series[left] : null;
        const rightCandidate = right >= 0 ? series[right] : null;
        if (!leftCandidate || !rightCandidate)
            return [leftCandidate || rightCandidate, leftCandidate ? left : right];

        const leftProximity = leftCandidate ? Math.abs(timepoint - leftCandidate.timepoint) : Number.MAX_SAFE_INTEGER;
        const rightProximity = rightCandidate ? Math.abs(timepoint - rightCandidate.timepoint) : Number.MAX_SAFE_INTEGER;
        return leftProximity < rightProximity ? [leftCandidate, left] : [rightCandidate, right];
    }
    private static toFixedSeries(series: { timepoint: number }[], from: number, to: number, interval: number, extend: (from: any) => any): ({ timepoint: number } | any)[] {
        if (!series.length || to - from <= 0 || !interval)
            return series;
        
        const count = Math.ceil((to - from) / interval);
        if (series.length == count)
            return series;
        
        let result = [], prev: any = null;
        while (result.length != count) {
            const timepoint: number = to - interval * result.length;
            const [next] = this.toBestSeriesItem(series, timepoint);
            const item = { ...(prev != null && prev == next ? extend(next || { }) : next || { }) };
            item.timepoint = timepoint;
            prev = next;
            result.push(item);
        } 
        return result.reverse();
    }
    private static async resultOf<T>(result: Promise<T>): Promise<T> {
        try {
            return await result;
        } catch (exception: any) {
            Log.info('storage error:', exception);
            throw exception;
        }
    }
    private static toNull<T>(value: T): T | null {
        if (value == 'NULL')
            return null;

        return value;
    }
}