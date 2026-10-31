import { BigNumber } from "bignumber.js";
import { AssetId, ByteUtil, Hashing, Pubkeyhash, UiUtil, Signing, Spot, Uint256, Whitelist, Seckey } from 'tangentsdk';
import { MarketPolicy, Contract, ContractType, ContractParameters, DlpParameters, AmsParameters, Order, OrderCondition, OrderPolicy, OrderSide, Trade, AggregatedPair, AggregatedTrade, AggregatedLevel, AggregatedLog, Block, RefType, Pool, Depth, DelegatedPool, PseudoDelegatedPool, PseudoDelegatedState, Balance, Booking, BookingStatus, PseudoEvent, PreattestationEvent, BroadcastEvent, AttestationEvent } from './../types';
import { Log } from './../logging';
import { Common } from './../common';
import { Blockchain, EventInfo } from './blockchain';
import { Quotes, symbolOf } from './market';
import NodeCache from 'node-cache';
import pq from 'postgres';
import os from 'os';

const BOOKING_STATUS_EDGES: Record<string, string[]> = {
    [BookingStatus.AwaitingDeposit]: [BookingStatus.FinalizingDeposit, BookingStatus.AwaitingExecution, BookingStatus.Expired],
    [BookingStatus.FinalizingDeposit]: [BookingStatus.AwaitingExecution, BookingStatus.AwaitingDeposit, BookingStatus.Expired],
    [BookingStatus.AwaitingExecution]: [BookingStatus.AwaitingWithdrawal, BookingStatus.AwaitingDeposit, BookingStatus.Expired],
    [BookingStatus.AwaitingWithdrawal]: [BookingStatus.FinalizingWithdrawal, BookingStatus.FailingWithdrawal, BookingStatus.AwaitingExecution],
    [BookingStatus.FinalizingWithdrawal]: [BookingStatus.Finalized, BookingStatus.FailingWithdrawal, BookingStatus.AwaitingWithdrawal],
    [BookingStatus.FailingWithdrawal]: [BookingStatus.AwaitingWithdrawal, BookingStatus.FinalizingWithdrawal],
    [BookingStatus.Finalized]: [],
    [BookingStatus.Expired]: []
};

const canTransition = (from: string, to: string) => (BOOKING_STATUS_EDGES[from] || []).includes(to);

export type PriceDescriptors = Record<string, { whitelist: boolean, base: string | null, price: { open: BigNumber | null, close: BigNumber | null } }>;

export type Connection = pq.TransactionSql;

export enum Notification {
    AuthorizerResponse = 'response:authorizer',
    ChainUpdate = 'update:chain',
    ContractUpdate = 'update:contract',
    DelegatedPoolUpdate = 'update:delegated-pool',
    OrderUpdate = 'update:order',
    PoolUpdate = 'update:pool',
    TradeUpdate = 'update:trade',
    LevelUpdate = 'update:level',
    BookingUpdate = 'update:booking'
}

export type NotificationData = {
    channelId?: string;
    accounts?: string[];
    args: Record<string, any>
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
    output: { min: BigNumber, max: BigNumber },
    capped: boolean
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
    intervals?: number[];
    signer?: {
        secretKey?: string;
        simulation?: boolean;
        confirmTimeout?: number;
        maxGasPrice?: number;
    }
}

export type Caches = {
    assetIdToPolyAssetId: NodeCache;
    assetHashToAssetId: NodeCache;
    assetIdsToPairId: NodeCache;
    assetPairIdToAssets: NodeCache;
    accountHashToAssetId: NodeCache;
}

export type Sequencer = {
    secretKey: Seckey,
    address: string,
    simulation: boolean,
    confirmTimeout: number,
    maxGasPrice: number | null
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
    private static config: pq.Options<any> | null = null;
    static listeners: Record<string, pq.ListenMeta> = { };
    static intervals: number[] = [2628000, 604800, 259200, 86400, 14400, 3600, 1800, 900, 300, 60];
    static connection: pq.Sql;
    static cache: Caches;
    static sequencer: Sequencer | null = null;

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
        const signer = config.signer; this.sequencer = null;
        if (signer != null && typeof signer.secretKey == 'string') {
            const secretKey = Signing.decodeSecretKey(signer.secretKey);
            if (secretKey == null)
                throw new Error('invalid signer secret key (read-only mode)');

            const address = Signing.encodeAddress(Signing.derivePublicKeyHash(Signing.derivePublicKey(secretKey)));
            if (address == null)
                throw new Error('failed to derive signer address (read-only mode)');

            this.sequencer = {
                secretKey: secretKey,
                address: address,
                simulation: signer.simulation === true,
                confirmTimeout: typeof signer.confirmTimeout == 'number' ? signer.confirmTimeout : 120,
                maxGasPrice: typeof signer.maxGasPrice == 'number' ? signer.maxGasPrice : null
            };
            Log.info(`sequencer ${address}${this.sequencer.simulation ? ' (simulation mode)' : ''}`);
        } else {
            this.sequencer = null;
        }

        const options: pq.Options<any> = {
            host: config.host || 'localhost',
            port: config.port || 19419,
            user: config.user,
            pass: config.password,
            database: config.database || 'ts',
            max: config.connections || Math.max(4, os.cpus().length),
            connection: {
                application_name: config.application || 'ts',
            }
        };
        const sql = pq({
            ...options,
            onnotice: (notice: pq.Notice) => {
                Log.info('storage notice:', notice['message'] || notice);
            },
            debug: (connection: number, query: string, parameters: any[], _: any[]) => {
                Log.query(`storage query (parameters: ${parameters}, connections: ${connection}):\n`, query);
            }
        });
        this.config = options;

        const result = await this.resultOf(sql`SELECT COUNT(1) FROM pg_extension WHERE extname = 'timescaledb'`);
        if (result[0]['count'] < 1)
            throw new Error('postgresql database must have \'timescaledb\' extension loaded');

        this.connection = sql;
        this.intervals = (config.intervals || this.intervals).sort((a, b) => a - b);
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

        CREATE TABLE IF NOT EXISTS contracts
        (
            id BIGSERIAL,
            type TEXT NOT NULL CHECK (type IN ('dex', 'dlp', 'uap', 'ams')),
            account_id BIGINT NOT NULL UNIQUE REFERENCES accounts (id) ON DELETE CASCADE,
            deployer_account_id BIGINT REFERENCES accounts (id) ON DELETE CASCADE,
            block_number BIGINT REFERENCES blocks (block_number) ON DELETE CASCADE,
            parameters JSONB NOT NULL DEFAULT '{}'::jsonb,
            synced BOOLEAN NOT NULL DEFAULT FALSE,
            PRIMARY KEY (id),
            UNIQUE (type)
        );

        CREATE TABLE IF NOT EXISTS poly_assets
        (
            asset_id BIGINT NOT NULL REFERENCES assets (id) ON DELETE CASCADE,
            poly_asset_id BIGINT NOT NULL REFERENCES assets (id) ON DELETE CASCADE,
            PRIMARY KEY (asset_id)
        );

        CREATE TABLE IF NOT EXISTS tiers
        (
            account_id BIGINT NOT NULL REFERENCES accounts (id) ON DELETE CASCADE,
            asset_id BIGINT NOT NULL REFERENCES assets (id) ON DELETE CASCADE,
            block_number BIGINT NOT NULL REFERENCES blocks (block_number) ON DELETE CASCADE,
            volume NUMERIC(96, 18) NOT NULL,
            maker_fee NUMERIC(96, 18) NOT NULL,
            taker_fee NUMERIC(96, 18) NOT NULL,
            PRIMARY KEY (account_id, asset_id)
        );

        CREATE TABLE IF NOT EXISTS orders
        (
            id BIGSERIAL,
            order_id BYTEA NOT NULL,
            pair_id BIGINT NOT NULL REFERENCES pairs (id) ON DELETE CASCADE,
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
            UNIQUE (order_id)
        );
        CREATE INDEX IF NOT EXISTS orders_order_id ON orders USING hash (order_id);

        CREATE TABLE IF NOT EXISTS pools
        (
            id BIGSERIAL,
            pool_id BYTEA NOT NULL,
            pair_id BIGINT NOT NULL REFERENCES pairs (id) ON DELETE CASCADE,
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
            UNIQUE (pool_id)
        );
        CREATE INDEX IF NOT EXISTS pools_pool_id ON pools USING hash (pool_id);
        CREATE INDEX IF NOT EXISTS pools_price_not_initial_price ON pools ((price <> initial_price));

        CREATE TABLE IF NOT EXISTS delegated_pools
        (
            id BIGSERIAL,
            pair_id BIGINT NOT NULL REFERENCES pairs (id) ON DELETE CASCADE,
            account_id BIGINT NOT NULL REFERENCES accounts (id) ON DELETE CASCADE,
            block_number BIGINT NOT NULL REFERENCES blocks (block_number) ON DELETE CASCADE,
            reward_value NUMERIC(96, 18) NOT NULL,
            initial_primary_value NUMERIC(96, 18) NOT NULL,
            initial_secondary_value NUMERIC(96, 18) NOT NULL,
            primary_value NUMERIC(96, 18) NOT NULL,
            secondary_value NUMERIC(96, 18) NOT NULL,
            active BOOLEAN NOT NULL,
            PRIMARY KEY (id),
            UNIQUE (pair_id, account_id)
        );

        CREATE TABLE IF NOT EXISTS depths
        (
            pair_id BIGINT NOT NULL REFERENCES pairs (id) ON DELETE CASCADE,
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
        SELECT set_integer_now_func('trades', 'system_clock', replace_if_exists => true);
        ALTER TABLE trades SET
        (
            timescaledb.compress,
            timescaledb.compress_segmentby = 'pair_id',
            timescaledb.compress_orderby = 'time DESC'
        );
        SELECT add_compression_policy('trades', 604800000, if_not_exists => true);

        CREATE TABLE IF NOT EXISTS bookings
        (
            id BIGSERIAL,
            asset_id BIGINT NOT NULL REFERENCES assets (id) ON DELETE CASCADE,
            amount NUMERIC(96, 18) NOT NULL,
            public_key TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'awaiting_deposit',
            expires_block BIGINT NOT NULL,
            tx_hash BYTEA NOT NULL,
            block_number BIGINT NOT NULL REFERENCES blocks (block_number) ON DELETE CASCADE,
            routing_address TEXT DEFAULT NULL,
            deposit_id TEXT DEFAULT NULL,
            deposit_hash BYTEA DEFAULT NULL,
            deposit_source TEXT DEFAULT NULL,
            deposit_block BIGINT DEFAULT NULL,
            deposit_payer_account_id BIGINT DEFAULT NULL,
            deposit_preattested_time BIGINT DEFAULT NULL,
            withdraw_hash BYTEA DEFAULT NULL,
            bridge_hash BYTEA DEFAULT NULL,
            out_asset_id BIGINT REFERENCES assets (id) ON DELETE SET NULL,
            out_amount NUMERIC(96, 18) DEFAULT NULL,
            broadcast_hash BYTEA DEFAULT NULL,
            proof_transaction_id TEXT DEFAULT NULL,
            relay_error TEXT DEFAULT NULL,
            attestate_hash BYTEA DEFAULT NULL,
            PRIMARY KEY (id),
            UNIQUE (tx_hash)
        );
        CREATE UNIQUE INDEX IF NOT EXISTS bookings_active_handle ON bookings (asset_id, amount) WHERE status NOT IN ('finalized', 'expired');
        CREATE INDEX IF NOT EXISTS bookings_lookup ON bookings (asset_id, amount);
        CREATE INDEX IF NOT EXISTS bookings_status ON bookings (status);

        CREATE MATERIALIZED VIEW IF NOT EXISTS pairs_view AS (
            WITH timings AS (
                SELECT
                    (SELECT EXTRACT(EPOCH FROM CURRENT_DATE::TIMESTAMP)::BIGINT * 1000) AS min_time_24h,
                    (SELECT EXTRACT(EPOCH FROM CURRENT_DATE::TIMESTAMP - INTERVAL '180 days')::BIGINT * 1000) AS min_time_180d,
                    (SELECT EXTRACT(EPOCH FROM CURRENT_DATE::TIMESTAMP + INTERVAL '1 day')::BIGINT * 1000) AS max_time
            )
            SELECT
                pairs.id,
                COALESCE((SELECT SUM(price * quantity) FROM trades WHERE pair_id = pairs.id AND time BETWEEN timings.min_time_180d AND timings.max_time AND maker_order_id IS NOT NULL), 0.0) AS order_volume,
                COALESCE((SELECT SUM(price * quantity) FROM trades WHERE pair_id = pairs.id AND time BETWEEN timings.min_time_180d AND timings.max_time AND maker_pool_id IS NOT NULL), 0.0) AS pool_volume,
                (SELECT ARRAY[price, time] FROM trades WHERE pair_id = pairs.id AND time BETWEEN timings.min_time_24h AND timings.max_time ORDER BY time ASC LIMIT 1) AS open_price_and_time,
                (SELECT ARRAY[MIN(price), MAX(price)] FROM trades WHERE pair_id = pairs.id AND time BETWEEN timings.min_time_24h AND timings.max_time LIMIT 1) AS low_price_and_high_price,
                (SELECT ARRAY[price, time] FROM trades WHERE pair_id = ppair.id AND time BETWEEN timings.min_time_24h AND timings.max_time ORDER BY time ASC LIMIT 1) AS psynthetic_open_price_and_time,
                (SELECT ARRAY[price, time] FROM trades WHERE pair_id = spair.id AND time BETWEEN timings.min_time_24h AND timings.max_time ORDER BY time ASC LIMIT 1) AS ssynthetic_open_price_and_time,
                (SELECT ARRAY[MIN(price), MAX(price)] FROM trades WHERE pair_id = ppair.id AND time BETWEEN timings.min_time_24h AND timings.max_time LIMIT 1) AS psynthetic_low_price_and_high_price,
                (SELECT ARRAY[MIN(price), MAX(price)] FROM trades WHERE pair_id = spair.id AND time BETWEEN timings.min_time_24h AND timings.max_time LIMIT 1) AS ssynthetic_low_price_and_high_price
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
                    (SELECT EXTRACT(EPOCH FROM CURRENT_DATE::TIMESTAMP - INTERVAL '180 days')::BIGINT * 1000) AS min_time,
                    (SELECT EXTRACT(EPOCH FROM CURRENT_DATE::TIMESTAMP + INTERVAL '1 day')::BIGINT * 1000) AS max_time
            )
            SELECT
                pools.id,
                COALESCE((SELECT SUM(price * quantity) FROM trades WHERE pair_id = pools.pair_id AND time BETWEEN timings.min_time AND timings.max_time AND maker_pool_id = pools.id), 0.0) AS volume
            FROM pools
                INNER JOIN timings ON TRUE
            WHERE initial_price <> price
        );
        CREATE MATERIALIZED VIEW IF NOT EXISTS delegators_view AS (
            WITH sources AS (
                SELECT
                    contracts.id,
                    contracts.account_id,
                    delegated_pools.pair_id
                FROM delegated_pools
                    INNER JOIN contracts ON contracts.type = 'dlp'
                GROUP BY contracts.id, contracts.account_id, delegated_pools.pair_id
            ), timings AS (
                SELECT
                    (SELECT EXTRACT(EPOCH FROM CURRENT_DATE::TIMESTAMP - INTERVAL '180 days')::BIGINT * 1000) AS min_time,
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
        );`.simple());
        await this.deployPriceSeries(sql);
    }
    private static async deployPriceSeries(connection?: pq.Sql | pq.TransactionSql): Promise<void> {
        const sql = connection || this.connection;
        const intervals = this.intervals.filter((v, index, self) => Number.isInteger(v) && v > 0 && self.indexOf(v) == index).sort((a, b) => a - b);
        const sources: number[] = new Array(intervals.length).fill(0);
        const fingerprints: string[] = new Array(intervals.length);
        const present: boolean[] = new Array(intervals.length).fill(false);
        const found: (string | null)[] = new Array(intervals.length).fill(null);
        for (let i = 0; i < intervals.length; i++) {
            for (let j = i - 1; j >= 0; j--) {
                if (intervals[i] % intervals[j] == 0) {
                    sources[i] = intervals[j];
                    break;
                }
            }

            fingerprints[i] = `tangent:price-series:v1:${intervals[i] * 1000}:${sources[i] > 0 ? 'trades_' + sources[i] + '_view' : 'trades'}`;
            const existing = await this.resultOf(sql`SELECT to_regclass(${'trades_' + intervals[i] + '_view'}) IS NOT NULL AS present, obj_description(to_regclass(${'trades_' + intervals[i] + '_view'}), 'pg_class') AS fingerprint`);
            present[i] = existing.length > 0 && existing[0]['present'] === true;
            found[i] = existing.length > 0 ? existing[0]['fingerprint'] : null;
        }

        const rebuildFrom = fingerprints.findIndex((v, index) => present[index] && found[index] !== v);
        for (let j = intervals.length - 1; j >= 0 && rebuildFrom >= 0 && j >= rebuildFrom; j--) {
            await this.resultOf(sql.unsafe(`DROP MATERIALIZED VIEW IF EXISTS trades_${intervals[j]}_view`));
            present[j] = false;
        }

        for (let i = 0; i < intervals.length; i++) {
            const interval = intervals[i];
            const bucket = interval * 1000;
            const source = sources[i];
            const name = 'trades_' + interval + '_view';
            await this.resultOf(sql.unsafe((source > 0 ? `
            CREATE MATERIALIZED VIEW IF NOT EXISTS ${name} WITH (timescaledb.continuous) AS
            SELECT
                pair_id,
                time_bucket(${bucket}, timepoint) AS timepoint,
                SUM(volume) AS volume,
                FIRST(open, timepoint) AS open,
                MIN(low) AS low,
                MAX(high) AS high,
                LAST(close, timepoint) AS close
            FROM trades_${source}_view
            GROUP BY pair_id, time_bucket(${bucket}, timepoint)` : `
            CREATE MATERIALIZED VIEW IF NOT EXISTS ${name} WITH (timescaledb.continuous) AS
            SELECT
                pair_id,
                time_bucket(${bucket}, time) AS timepoint,
                SUM(quantity) AS volume,
                FIRST(price, time) AS open,
                MIN(price) AS low,
                MAX(price) AS high,
                LAST(price, time) AS close
            FROM trades
            GROUP BY pair_id, time_bucket(${bucket}, time)`)));
            await this.resultOf(sql.unsafe(`COMMENT ON VIEW ${name} IS '${fingerprints[i].replace(/'/g, "''")}'`));
            await this.resultOf(sql.unsafe(`CREATE INDEX IF NOT EXISTS ${name}_pair_id_timepoint ON ${name} (pair_id, timepoint DESC)`));
            const policies = await this.resultOf(sql`SELECT 1 AS configured FROM timescaledb_information.jobs WHERE proc_name = 'policy_refresh_continuous_aggregate' AND hypertable_schema = 'public' AND hypertable_name = ${name} AND config ->> 'end_offset' = ${String(bucket)} AND schedule_interval = INTERVAL '1 minutes'`);
            if (policies.length < 1) {
                await this.resultOf(sql.unsafe(`SELECT remove_continuous_aggregate_policy('${name}', if_not_exists => true)`));
                await this.resultOf(sql.unsafe(`SELECT add_continuous_aggregate_policy('${name}', start_offset => ${Math.max(bucket * 4, 86400000)}, end_offset => ${bucket}, schedule_interval => INTERVAL '1 minutes')`));
            }
            if (!present[i])
                await this.resultOf(sql.unsafe(`CALL refresh_continuous_aggregate('${name}', NULL, NULL)`));
        }
    }
    static async isolate<T>(callback: (sql: pq.TransactionSql) => T | Promise<T>) {
        return await this.connection.begin(callback);
    }
    static async listen(channel: Notification, callback: (notification: NotificationData) => any, connection?: pq.TransactionSql): Promise<void> {
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
    static async notify(channel: Notification, notification: NotificationData, connection?: pq.TransactionSql): Promise<boolean> {
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
    static async dispatchEvents(block: { number: number, time: Date }, contract: { account: string, type: string }, events: EventInfo[], connection?: pq.TransactionSql): Promise<void> {
        const accounts: Record<string, Uint256> = { };
        if (!accounts[contract.account]) {
            const accountId = await this.getAccountIdByAddress(contract.account, false, connection);
            if (!accountId)
                throw new Error('cannot decode contract account ' + contract.account);

            accounts[contract.account] = accountId;
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
                case ContractType.DEX: {
                    switch (log.event.type) {
                        case Spot.DEX.Events.Config: {
                            try {
                                const market = await Blockchain.call(contract.account, UiUtil.toFunction(Spot.DEX.paramsOf), []);
                                if (market != null) {
                                    if (!accounts[market.deployer_account]) {
                                        const accountId = await this.getAccountIdByAddress(market.deployer_account, false, connection);
                                        if (!accountId)
                                            throw new Error('cannot decode deployer account ' + market.deployer_account);
                                        accounts[market.deployer_account] = accountId;
                                    }

                                    const result = await this.setContract({
                                        type: ContractType.DEX,
                                        accountId: accounts[contract.account],
                                        deployerAccountId: accounts[market.deployer_account],
                                        blockNumber: block.number,
                                        parameters: {
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
                                        }
                                    }, connection);
                                    if (result != null)
                                        await this.notify(Notification.ContractUpdate, { args: { type: ContractType.DEX, account: contract.account } }, connection);
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
                                    switch (log.method ? UiUtil.toFunction(log.method) : null) {
                                        case UiUtil.toFunction(Spot.DEX.marketOrder):
                                            pseudo = {
                                                ...orderParameters(),
                                                condition: OrderCondition.Market,
                                                slippage: Common.bn(log.args[4]),
                                            };
                                            break;
                                        case UiUtil.toFunction(Spot.DEX.limitOrder):
                                            pseudo = {
                                                ...orderParameters(),
                                                condition: OrderCondition.Limit,
                                                price: Common.bn(log.args[4]),
                                            };
                                            break;
                                        case UiUtil.toFunction(Spot.DEX.stopOrder):
                                            pseudo = {
                                                ...orderParameters(),
                                                condition: OrderCondition.Stop,
                                                stopPrice: Common.bn(log.args[4]),
                                                slippage: Common.bn(log.args[5])
                                            };
                                            break;
                                        case UiUtil.toFunction(Spot.DEX.stopLimitOrder):
                                            pseudo = {
                                                ...orderParameters(),
                                                condition: OrderCondition.StopLimit,
                                                stopPrice: Common.bn(log.args[4]),
                                                price: Common.bn(log.args[5])
                                            };
                                            break;
                                        case UiUtil.toFunction(Spot.DEX.trailingStopOrder):
                                            pseudo = {
                                                ...orderParameters(),
                                                condition: OrderCondition.TrailingStop,
                                                stopPrice: Common.bn(log.args[4]),
                                                slippage: Common.bn(log.args[5]),
                                                trailingStep: Common.bn(log.args[6]),
                                                trailingDistance: Common.bn(log.args[7]),
                                            };
                                            break;
                                        case UiUtil.toFunction(Spot.DEX.trailingStopLimitOrder):
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
                                    switch (log.method ? UiUtil.toFunction(log.method) : null) {
                                        case UiUtil.toFunction(Spot.DEX.depositPool):
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

                                const tier = await Blockchain.call(contract.account, UiUtil.toFunction(Spot.DEX.assetOf), [asset.toUint256()]);
                                const assetId = await this.getAssetIdByHash(asset, 'trusted', connection);
                                if (!assetId)
                                    throw new Error('Failed to get asset id');

                                await this.setPolyAsset(assetId, contract.account, tier ? tier.symbol || null : null, connection);
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
                case ContractType.DLP: {
                    switch (log.event.type) {
                        case Spot.DLP.Events.Config: {
                            try {
                                const delegator = await Blockchain.call(contract.account, UiUtil.toFunction(Spot.DLP.paramsOf), []);
                                if (delegator != null) {
                                    if (!accounts[delegator.deployer_account]) {
                                        const accountId = await this.getAccountIdByAddress(delegator.deployer_account, false, connection);
                                        if (!accountId)
                                            throw new Error('cannot decode deployer account ' + delegator.deployer_account);
                                        accounts[delegator.deployer_account] = accountId;
                                    }
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

                                    const result = await this.setContract({
                                        type: ContractType.DLP,
                                        accountId: accounts[contract.account],
                                        deployerAccountId: accounts[delegator.deployer_account],
                                        blockNumber: block.number,
                                        parameters: {
                                            rewardEmission: Common.bn(delegator.reward_emission) || new BigNumber(0),
                                            rewardBalance: Common.bn(delegator.reward_balance) || new BigNumber(0),
                                            permissions: permissions.filter((x: any) => x.primaryAssetId != null && x.secondaryAssetId != null)
                                        }
                                    }, connection);
                                    if (result != null)
                                        await this.notify(Notification.ContractUpdate, { args: { type: ContractType.DLP, account: contract.account } }, connection);
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
                                    const dlp = await this.getContractByType(ContractType.DLP, connection);
                                    if (dlp == null)
                                        throw new Error('dlp contract not registered: ' + contract.account);

                                    const primaryAssetId = await this.getAssetIdByHash(primaryAsset, 'trusted', connection);
                                    if (!primaryAssetId)
                                        throw new Error('cannot find primary asset of contract account ' + contract.account);

                                    const secondaryAssetId = await this.getAssetIdByHash(secondaryAsset, 'trusted', connection);
                                    if (!secondaryAssetId)
                                        throw new Error('cannot find secondary asset of contract account ' + contract.account);

                                    const pairId = await this.getPairByAssetIds(primaryAssetId, secondaryAssetId, true, connection);
                                    if (!pairId)
                                        throw new Error('cannot find asset pair of contract account ' + contract.account);

                                    const delegatedPools = await this.getAllDelegatedPoolsByPair(pairId)
                                    for (let i = 0; i < delegatedPools.length; i++) {
                                        const delegatedPool = delegatedPools[i];
                                        let delegatedPoolId: Uint256 | null = null;
                                        let owner: string | null = null;
                                        let active = true;
                                        try {
                                            const account = await this.getAccountHashById(delegatedPool.accountId, connection);
                                            if (!account)
                                                throw new Error('Failed to get delegated pool account id: ' + delegatedPool.accountId);

                                            owner = Signing.encodeAddress(account);
                                            const share = await Blockchain.call(contract.account, UiUtil.toFunction(Spot.DLP.shareOf), [primaryAsset.toUint256(), secondaryAsset.toUint256(), owner]);
                                            const result = await this.setDelegatedPool({
                                                pairId: pairId,
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
                                            const prevDelegatedPool = await this.getDelegatedPoolByHandle(pairId, delegatedPool.accountId, connection);
                                            if (prevDelegatedPool) {
                                                delegatedPoolId = prevDelegatedPool.id;
                                                prevDelegatedPool.blockNumber = block.number;
                                                prevDelegatedPool.active = active = false;
                                                await this.setDelegatedPool(prevDelegatedPool, false, connection);
                                            }
                                        }

                                        if (owner && delegatedPoolId) {
                                            await this.notify(Notification.DelegatedPoolUpdate, {
                                                accounts: [owner],
                                                args: { delegatedPoolId: delegatedPoolId, active: active }
                                            }, connection);
                                        }
                                    }

                                    Log.info(`exchange ${contract.account} delegated pool batch update (pair_id: ${pairId.toString()}): ${delegatedPools.length} updates`);
                                } catch (exception) {
                                    Log.error(`exchange ${contract.account} delegated pool batch update error (primary_asset: ${primaryAsset.id}, secondary_asset: ${secondaryAsset.id}):`, exception);
                                }
                            }

                            if (!ownerPubkeyhash.equals(new Pubkeyhash())) {
                                let active = true;
                                try {
                                    const ownerId = owner ? await this.getAccountIdByAddress(owner, false, connection) : null;
                                    if (!ownerId)
                                        throw new Error('Failed to get owner account id: ' + owner);

                                    const dlp = await this.getContractByType(ContractType.DLP, connection);
                                    if (dlp == null)
                                        throw new Error('dlp contract not registered: ' + contract.account);

                                    const primaryAssetId = await this.getAssetIdByHash(primaryAsset, 'trusted', connection);
                                    if (!primaryAssetId)
                                        throw new Error('cannot find primary asset of contract account ' + contract.account);

                                    const secondaryAssetId = await this.getAssetIdByHash(secondaryAsset, 'trusted', connection);
                                    if (!secondaryAssetId)
                                        throw new Error('cannot find secondary asset of contract account ' + contract.account);

                                    const pairId = await this.getPairByAssetIds(primaryAssetId, secondaryAssetId, true, connection);
                                    if (!pairId)
                                        throw new Error('cannot find asset pair of contract account ' + contract.account);

                                    let delegatedPoolId: Uint256 | null = null;
                                    try {
                                        const share = await Blockchain.call(contract.account, UiUtil.toFunction(Spot.DLP.shareOf), [primaryAsset.toUint256(), secondaryAsset.toUint256(), owner]);
                                        const result = await this.setDelegatedPool({
                                            pairId: pairId,
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
                                        const prevDelegatedPool = await this.getDelegatedPoolByHandle(pairId, ownerId, connection);
                                        if (prevDelegatedPool) {
                                            delegatedPoolId = prevDelegatedPool.id;
                                            prevDelegatedPool.blockNumber = block.number;
                                            prevDelegatedPool.active = active = false;
                                            await this.setDelegatedPool(prevDelegatedPool, false, connection);
                                        }
                                    }

                                    await this.notify(Notification.DelegatedPoolUpdate, {
                                        accounts: owner ? [owner] : undefined,
                                        args: { delegatedPoolId: delegatedPoolId || new Uint256(0), active: active }
                                    }, connection);
                                    Log.info(`exchange ${contract.account} delegated pool update (pair_id: ${pairId.toString()}, owner: ${ownerId.toString()})`);
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
                case ContractType.AMS: {
                    switch (log.event.type) {
                        case Spot.AMS.Events.Config: {
                            try {
                                const params = await Blockchain.call(contract.account, UiUtil.toFunction(Spot.AMS.paramsOf), []);
                                if (params != null) {
                                    if (!accounts[params.deployer_account]) {
                                        const deployerAccountId = await this.getAccountIdByAddress(params.deployer_account, false, connection);
                                        if (!deployerAccountId)
                                            throw new Error('cannot decode deployer account ' + params.deployer_account);
                                        accounts[params.deployer_account] = deployerAccountId;
                                    }

                                    const result = await this.setContract({
                                        type: ContractType.AMS,
                                        accountId: accounts[contract.account],
                                        deployerAccountId: accounts[params.deployer_account],
                                        blockNumber: block.number,
                                        parameters: {
                                            dex: params.dex_account,
                                            feeOverflow: Common.bn(params.fee_overflow) || new BigNumber(0),
                                            bookValidityBlocks: Common.num(params.book_validity_blocks) || 0,
                                            depositValidityBlocks: Common.num(params.deposit_validity_blocks) || 0,
                                            depositPrecision: Common.num(params.deposit_precision) ?? 6
                                        }
                                    }, connection);
                                    if (result != null)
                                        await this.notify(Notification.ContractUpdate, { args: { type: ContractType.AMS, account: contract.account } }, connection);
                                }

                                Log.info(`exchange ${contract.account} ams config update`);
                            } catch (exception) {
                                Log.error(`exchange ${contract.account} ams config update error:`, exception);
                            }
                            break;
                        }
                        case Spot.AMS.Events.BookingRef: {
                            try {
                                const amsContract = await this.getAmsContract(connection);
                                if (amsContract == null)
                                    throw new Error('ams contract not registered (config event missing): ' + contract.account);

                                const asset = new AssetId(log.event.args[0]);
                                const amount = Common.bn(log.event.args[1]) || new BigNumber(0);
                                if (amount.isLessThanOrEqualTo(0))
                                    throw new Error('invalid booked amount');

                                const publicKey = Array.isArray(log.args) && log.args.length > 2 ? String(log.args[2]) : '';
                                if (publicKey.length < 1)
                                    throw new Error('booking transaction public key missing');

                                await this.expireBookings(block.number, connection);
                                await this.setBooking(asset, {
                                    amount: amount,
                                    publicKey: publicKey,
                                    expiresBlock: block.number + amsContract.parameters.bookValidityBlocks,
                                    txHash: Common.u256(log.hash) || new Uint256(),
                                    blockNumber: block.number
                                }, connection);
                                Log.info(`exchange ${contract.account} ams booking: ${asset.id} ${amount.toString()} (public key: ${publicKey}, expires: ${block.number + amsContract.parameters.bookValidityBlocks}) @ block ${block.number}`);
                            } catch (exception) {
                                Log.error(`exchange ${contract.account} ams booking update error:`, exception);
                            }
                            break;
                        }
                        case Spot.AMS.Events.SwapRef: {
                            try {
                                const amsContract = await this.getAmsContract(connection);
                                if (amsContract == null)
                                    throw new Error('ams contract not registered: ' + contract.account);

                                const args = log.event.args;
                                if (!Array.isArray(args) || args.length < 4)
                                    throw new Error('unexpected swap_ref event layout');

                                const inputAssetId = await this.getAssetIdByHash(new AssetId(args[0]), 'trusted', connection);
                                const inputAmount = Common.bn(args[1]);
                                if (inputAssetId == null || inputAmount == null)
                                    throw new Error('cannot resolve swap_ref input');
                                Log.info(`exchange ${contract.account} ams swap_ref: ${new AssetId(args[0]).id} ${inputAmount.toString()} @ block ${block.number}`);

                                const booking = await this.getBookingByHandle(inputAssetId, inputAmount.decimalPlaces(amsContract.parameters.depositPrecision ?? 6, BigNumber.ROUND_DOWN), null, connection);
                                if (booking == null)
                                    Log.info(`exchange ${contract.account} ams swap_ref without any booking (input: ${args[0]}, amount: ${inputAmount.toString()})`);
                                else if (booking.status != BookingStatus.AwaitingExecution)
                                    Log.info(`exchange ${contract.account} ams swap_ref on ${booking.status} booking (deposit: ${log.hash})`);

                                if (booking != null && booking.outAsset == null) {
                                    const output = await this.findSwapRefOutput(events, log.hash, connection);
                                    if (output != null)
                                        await this.setBookingOutput(booking.id, output.outAssetId, output.outAmount, connection);
                                }
                            } catch (exception) {
                                Log.error(`exchange ${contract.account} ams swap ref error:`, exception);
                            }
                            break;
                        }
                        case Spot.AMS.Events.SpentBooking: {
                            try {
                                const amsContract = await this.getAmsContract(connection);
                                if (amsContract == null)
                                    throw new Error('ams contract not registered: ' + contract.account);

                                if (!Array.isArray(log.event.args) || log.event.args.length < 6)
                                    throw new Error('unexpected spent_booking event layout');

                                const depositHash = Common.u256(Array.isArray(log.args) && log.args.length > 0 ? log.args[0] : null);
                                if (depositHash == null)
                                    throw new Error('deposit hash missing from spending transaction');

                                const withdrawHash = Common.u256(log.event.args[4]);
                                if (withdrawHash == null)
                                    throw new Error('withdraw hash missing from spent_booking event');

                                const expiresBlock = Common.num(log.event.args[3]) || 0;
                                const bridgeHash = Common.u256(log.event.args[5]);
                                let booking: Booking | null = await this.getBookingByDepositHash(depositHash, connection);

                                if (booking == null) {
                                    for (let j = 0; j < events.length && booking == null; j++) {
                                        const other = events[j];
                                        if (other.hash == log.hash && other.event.type == Spot.AMS.Events.SwapRef && Array.isArray(other.event.args) && other.event.args.length >= 4) {
                                            const inputAssetId = await this.getAssetIdByHash(new AssetId(other.event.args[0]), 'trusted', connection);
                                            const inputAmount = Common.bn(other.event.args[1]);
                                            if (inputAssetId != null && inputAmount != null)
                                                booking = await this.getBookingByHandle(inputAssetId, inputAmount.decimalPlaces(amsContract.parameters.depositPrecision ?? 6, BigNumber.ROUND_DOWN), null, connection);
                                        }
                                    }
                                }

                                if (booking == null)
                                    throw new Error('no matching AMS booking for spent booking (deposit: ' + depositHash.toHex() + ')');

                                if (booking.depositHash == null)
                                    await this.setBookingDepositFacts(booking.id, { depositHash: depositHash, source: 'mined', blockNumber: block.number }, connection);

                                if (booking.outAsset == null) {
                                    const output = await this.findSwapRefOutput(events, log.hash, connection);
                                    if (output != null)
                                        await this.setBookingOutput(booking.id, output.outAssetId, output.outAmount, connection);
                                }

                                await this.setBookingWithdrawal(booking.id, withdrawHash, bridgeHash, expiresBlock, booking.withdrawHash != withdrawHash.toHex(), connection);

                                if (booking.status == BookingStatus.AwaitingExecution || booking.status == BookingStatus.FailingWithdrawal)
                                    await this.updateBookingStatus(booking.id, BookingStatus.AwaitingWithdrawal, connection);
                                Log.info(`exchange ${contract.account} ams spent_booking: deposit ${depositHash.toHex()} withdraw ${withdrawHash.toHex()} (booking: ${booking.id}) @ block ${block.number}`);

                            } catch (exception) {
                                Log.error(`exchange ${contract.account} ams spent booking error:`, exception);
                            }
                            break;
                        }
                        case PseudoEvent.Preattestation: {
                            const row = log.event.args[0] as PreattestationEvent;
                            Log.info(`exchange ${contract.account} ams preattestation: ${row.txHash.toHex()} (${row.asset.id} ${row.amount.toString()}, ${row.source == 'mined' ? 'block ' + block.number : 'mempool'})`);
                            try {
                                const assetId = await this.getAssetIdByHash(row.asset, 'trusted', connection);
                                if (assetId == null) {
                                    Log.error(`ams preattestation: cannot find asset ${row.asset.id} (preattestation: ${row.txHash.toHex()})`);
                                    break;
                                }

                                const payerAccountId = row.payerAccount != null ? await this.getAccountIdByAddress(row.payerAccount, false, connection) : null;
                                let booking = row.txId != null ? await this.getBookingByDepositId(row.txId, connection) : null;
                                if (booking == null && row.source == 'mined')
                                    booking = await this.getBookingByDepositHash(row.txHash, connection);
                                if (booking == null && row.isSinglePayment) {
                                    const amsContract = await this.getAmsContract(connection);
                                    const paidAmount = row.amount.decimalPlaces(amsContract?.parameters.depositPrecision ?? 6, BigNumber.ROUND_DOWN);
                                    booking = await this.getBookingByHandle(assetId, paidAmount, [BookingStatus.AwaitingDeposit, BookingStatus.FinalizingDeposit], connection);
                                }

                                if (booking == null) {
                                    Log.info(`exchange ${contract.account} ams preattestation without any booking (preattestation: ${row.txHash.toHex()})`);
                                    break;
                                }

                                if (row.source == 'mined') {
                                    if (booking.depositBlock != null)
                                        break;
                                    if (booking.depositHash != null && booking.depositHash != row.txHash.toHex()) {
                                        Log.info(`exchange ${contract.account} ams deposit ignored (booking ${booking.id} already linked to ${booking.depositHash}): ${row.txHash.toHex()}`);
                                        break;
                                    }
                                    await this.setBookingDepositFacts(booking.id, {
                                        depositHash: row.txHash,
                                        depositId: row.txId ?? row.txHash.toHex(),
                                        source: 'mined',
                                        blockNumber: block.number,
                                        payerAccountId: payerAccountId
                                    }, connection);
                                    if (booking.status != BookingStatus.AwaitingExecution)
                                        await this.updateBookingStatus(booking.id, BookingStatus.AwaitingExecution, connection);
                                } else {
                                    if (row.txId == null || booking.depositHash != null || booking.depositId != null)
                                        break;
                                    await this.setBookingDepositFacts(booking.id, {
                                        depositId: row.txId,
                                        source: 'preattestation',
                                        payerAccountId: payerAccountId,
                                        preattestedTime: Math.floor(Date.now() / 1_000)
                                    }, connection);
                                    if (booking.status == BookingStatus.AwaitingDeposit)
                                        await this.updateBookingStatus(booking.id, BookingStatus.FinalizingDeposit, connection);
                                }
                            } catch (exception) {
                                Log.error(`exchange ${contract.account} ams preattestation ingest error (preattestation: ${row.txHash.toHex()}):`, exception);
                            }
                            break;
                        }
                        case PseudoEvent.Broadcast: {
                            const row = log.event.args[0] as BroadcastEvent;
                            Log.info(`exchange ${contract.account} ams broadcast: ${row.txHash.toHex()} (withdraw: ${row.withdrawHash.toHex()}${row.error != null ? ', error: ' + row.error : row.proofTransactionId != null ? ', proof: ' + row.proofTransactionId : ''}) @ block ${block.number}`);
                            try {
                                const bookings = await this.getBookingsByWithdrawHash(row.withdrawHash, null, connection);
                                for (let j = 0; j < bookings.length; j++) {
                                    const booking = bookings[j];
                                    if (row.proofTransactionId != null) {
                                        if (booking.broadcastHash == row.txHash.toHex() && booking.proofTransactionId == row.proofTransactionId && booking.status == BookingStatus.FinalizingWithdrawal)
                                            continue;
                                        await this.setBookingBroadcastProof(booking.id, row.txHash, row.proofTransactionId, connection);
                                        if (booking.status != BookingStatus.FinalizingWithdrawal && canTransition(booking.status, BookingStatus.FinalizingWithdrawal))
                                            await this.updateBookingStatus(booking.id, BookingStatus.FinalizingWithdrawal, connection);
                                    } else if (row.error != null) {
                                        if (booking.status == BookingStatus.FailingWithdrawal && booking.broadcastHash == row.txHash.toHex())
                                            continue;

                                        await this.setBookingBroadcastError(booking.id, row.txHash, row.error, connection);
                                        if (booking.status != BookingStatus.FailingWithdrawal && canTransition(booking.status, BookingStatus.FailingWithdrawal))
                                            await this.updateBookingStatus(booking.id, BookingStatus.FailingWithdrawal, connection);
                                    }
                                }
                            } catch (exception) {
                                Log.error(`exchange ${contract.account} ams broadcast ingest error (broadcast: ${row.txHash.toHex()}):`, exception);
                            }
                            break;
                        }
                        case PseudoEvent.Attestation: {
                            const row = log.event.args[0] as AttestationEvent;
                            Log.info(`exchange ${contract.account} ams attestation: ${row.txHash.toHex()} (hashdata: ${row.hashdata}) @ block ${block.number}`);
                            try {
                                const bookings = await this.getBookingsByProofTransactionId(row.hashdata, connection);
                                for (let j = 0; j < bookings.length; j++) {
                                    const booking = bookings[j];
                                    if (booking.status == BookingStatus.Finalized && booking.attestateHash == row.txHash.toHex())
                                        continue;
                                    await this.setBookingAttestate(booking.id, row.txHash, connection);
                                    if (booking.status != BookingStatus.Finalized)
                                        await this.updateBookingStatus(booking.id, BookingStatus.Finalized, connection, { force: true });
                                }

                                if (typeof row.hashdata == 'string' && row.hashdata.length > 0) {
                                    let depositBooking = await this.getBookingByDepositId(row.hashdata, connection);
                                    if (depositBooking == null && row.asset != null && row.amount != null) {
                                        const depositAssetId = await this.getAssetIdByHash(row.asset, 'trusted', connection);
                                        if (depositAssetId != null) {
                                            const amsContract = await this.getAmsContract(connection);
                                            const paidAmount = row.amount.decimalPlaces(amsContract?.parameters.depositPrecision ?? 6, BigNumber.ROUND_DOWN);
                                            depositBooking = await this.getBookingByHandle(depositAssetId, paidAmount, [BookingStatus.AwaitingDeposit, BookingStatus.FinalizingDeposit], connection);
                                        }
                                    }
                                    if (depositBooking != null && depositBooking.depositHash == null && (depositBooking.status == BookingStatus.AwaitingDeposit || depositBooking.status == BookingStatus.FinalizingDeposit)) {
                                        Log.info(`exchange ${contract.account} ams deposit vault transfer ${row.txHash.toHex()} linked to booking ${depositBooking.id} (off-chain: ${row.hashdata}, block: ${block.number})`);
                                        await this.setBookingDepositFacts(depositBooking.id, { depositHash: row.txHash, depositId: row.hashdata, source: 'mined', blockNumber: block.number }, connection);
                                        await this.updateBookingStatus(depositBooking.id, BookingStatus.AwaitingExecution, connection);
                                    }
                                }
                            } catch (exception) {
                                Log.error(`exchange ${contract.account} ams attestation ingest error (attestation: ${row.txHash.toHex()}):`, exception);
                            }
                            break;
                        }
                        default:
                            Log.error(`exchange ${contract.account} event is unknown (type: ${log.event.type}, block: ${block.number}, contract: ams):`, log.event.args);
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
                        order = await Blockchain.call(contract.account, UiUtil.toFunction(Spot.DEX.orderOf), [event.orderId]);
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

                    const pair = order ? await Blockchain.call(contract.account, UiUtil.toFunction(Spot.DEX.pairOf), [order.pair_id]) : { primary_asset: pseudoOrder.primaryAsset.toHex(), secondary_asset: pseudoOrder.secondaryAsset.toHex() };
                    if (!pair)
                        throw new Error('cannot find asset pair of contract account ' + contract.account);

                    const primaryAssetId = await this.getAssetIdByHash(new AssetId(pair.primary_asset), 'trusted', connection);
                    if (!primaryAssetId)
                        throw new Error('cannot find primary asset of contract account ' + contract.account);

                    const secondaryAssetId = await this.getAssetIdByHash(new AssetId(pair.secondary_asset), 'trusted', connection);
                    if (!secondaryAssetId)
                        throw new Error('cannot find secondary asset of contract account ' + contract.account);

                    const pairId = await this.getPairByAssetIds(primaryAssetId, secondaryAssetId, true, connection);
                    if (!pairId)
                        throw new Error('cannot find asset pair of contract account ' + contract.account);

                    const startingValue = BigNumber.max(pseudoOrder ? pseudoOrder.value : 0, order ? order.value : 0);
                    const currentValue = new BigNumber(order ? order.value : BigNumber.max(0, pseudoOrder.value.minus(pseudoOrder.side == OrderSide.Buy ? event.secondaryQuantity : event.primaryQuantity)));
                    result = await this.setOrder({
                        orderId: order ? Common.u256(order.id) || new Uint256(order.id) : new Uint256(pseudoOrder.transaction.hash),
                        pairId: pairId,
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
                            accounts: [account],
                            args: { orderId: result.id }
                        }, connection),
                        this.notify(Notification.LevelUpdate, {
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
                let active = true;
                try {
                    const pseudoPool: PseudoPool = event.pseudoRef as any;
                    try {
                        pool = await Blockchain.call(contract.account, UiUtil.toFunction(Spot.DEX.poolOf), [event.poolId]);
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

                    const pair = pool ? await Blockchain.call(contract.account, UiUtil.toFunction(Spot.DEX.pairOf), [pool.pair_id]) : { primary_asset: pseudoPool.primaryAsset.toHex(), secondary_asset: pseudoPool.secondaryAsset.toHex() };
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

                    const pairId = await this.getPairByAssetIds(primaryAssetId, secondaryAssetId, true, connection);
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
                        prevPool.active = active = false;
                        result = await this.setPool(prevPool, connection);
                        if (result != null) {
                            await this.setDepth({
                                poolId: result.id,
                                pairId: result.pairId,
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
                        accounts: [account],
                        args: { poolId: result.id, active: active }
                    }, connection)];
                    if (result.active && ((result.lastAskPrice.gt(0) && result.lastAskPrice.lt(maxPrice) && result.primaryValue.gt(0)) || (result.lastBidPrice.gt(0) && result.lastBidPrice.gt(minPrice) && result.secondaryValue.gt(0)))) {
                        events.push(this.notify(Notification.LevelUpdate, {
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

                const pairId: Uint256 = (makerOrder?.pairId || makerPool?.pairId || takerOrder?.pairId) as any;
                if (takerOrder && pairId.neq(takerOrder.pairId))
                    throw new Error('maker/taker order market mismatch of contract account ' + contract.account + '(maker_order_or_pool_id: ' + (event.makerOrderOrPoolId.toString() || 'null') + ', taker_order_id: ' + event.takerOrderId.toString() + ')');

                const assets = await this.getPairById(pairId, connection);
                if (!assets || !assets.primaryAsset || !assets.secondaryAsset)
                    throw new Error('cannot find assets of contract account ' + contract.account + '(order_id: ' + event.takerOrderId.toString() + ')');

                const base = assets && assets.secondaryAsset && Whitelist.has(assets.secondaryAsset.hash) ? Quotes.assetBaseOf(assets.secondaryAsset.hash) : null;
                const trade = {
                    pairId: pairId,
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
                    const pairId = await this.getPairByAssetIds(assets.primaryAsset.id, null, true, connection);
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
        await this.rollbackBookings(blockNumber, connection);
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
    static async setPolyAsset(assetId: Uint256, marketAccount: string, symbol: string | null, connection?: pq.TransactionSql): Promise<AssetId | null> {
        const sql = connection || this.connection;
        const polyAsset = symbol ? AssetId.fromHandle(new AssetId().chain || '', symbol, marketAccount) : null;
        await this.resultOf(sql`DELETE FROM poly_assets WHERE asset_id = ${assetId.toString()}`);
        if (!polyAsset)
            return null;

        const polyAssetId = await this.getAssetIdByHash(polyAsset, 'trusted', connection);
        if (!polyAssetId)
            return polyAsset;

        await this.resultOf(sql`INSERT INTO poly_assets (asset_id, poly_asset_id) VALUES (${assetId.toString()}, ${polyAssetId.toString()}) ON CONFLICT (asset_id) DO NOTHING`);
        return polyAsset;
    }
    static async getPolyAssetId(assetId: Uint256, connection?: pq.TransactionSql): Promise<Uint256 | null> {
        const sql = connection || this.connection;
        const key = assetId.toString();
        const cache = this.get(this.cache.assetIdToPolyAssetId, key);
        if (cache != null)
            return new Uint256(cache);

        let result = await this.resultOf(sql`SELECT poly_asset_id FROM poly_assets WHERE asset_id = ${assetId.toString()}`);
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
    static async getPairByAssetIds(primaryAssetId: Uint256 | null, secondaryAssetId: Uint256 | null, createIfNotExists: boolean, connection?: pq.TransactionSql): Promise<Uint256 | null> {
        const sql = connection || this.connection;
        if (primaryAssetId != null) {
            const polyPrimaryAssetId = await this.getPolyAssetId(primaryAssetId, connection);
            if (polyPrimaryAssetId) {
                primaryAssetId = polyPrimaryAssetId;
            }
        }

        if (secondaryAssetId != null) {
            const polySecondaryAssetId = await this.getPolyAssetId(secondaryAssetId, connection);
            if (polySecondaryAssetId) {
                secondaryAssetId = polySecondaryAssetId;
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
    static async getPairByAssetHashes(primaryAssetHash: AssetId | null, secondaryAssetHash: AssetId | null, verifyExistence: boolean, createIfNotExists: boolean, connection?: pq.TransactionSql): Promise<Uint256 | null> {
        const primaryAssetId = primaryAssetHash ? await this.getAssetIdByHash(primaryAssetHash, verifyExistence ? 'untrusted' : 'trusted', connection) : null;
        const secondaryAssetId = secondaryAssetHash ? await this.getAssetIdByHash(secondaryAssetHash, verifyExistence ? 'untrusted' : 'trusted', connection) : null;
        return await this.getPairByAssetIds(primaryAssetId, secondaryAssetId, createIfNotExists, connection);
    }
    static async getAggregatedPairs(pairId: Uint256 | null, connection?: pq.TransactionSql): Promise<AggregatedPair[]> {
        const sql = connection || this.connection;
        const result = await this.resultOf(sql`
        WITH results AS (
            SELECT
                pairs_view.*,
                pairs.*,
                passet.hash AS primary_asset,
                sasset.hash AS secondary_asset,
                COALESCE((SELECT SUM(last_price * last_quantity) FROM orders WHERE pair_id = pairs.id AND active = TRUE), 0.0) AS order_liquidity,
                COALESCE((SELECT SUM(COALESCE(last_ask_price, 0) * primary_value + secondary_value) FROM pools WHERE pair_id = pairs.id AND active = TRUE), 0.0) AS pool_liquidity,
                COALESCE((SELECT AVG(fee_rate) FROM pools WHERE pair_id = pairs.id AND active = TRUE), 0.0) AS pool_fee_rate,
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
            try {
                const stableQuoted = await this.resultOf(sql`
                SELECT
                    assets.hash,
                    sassets.hash AS quote_hash,
                    (SELECT price FROM trades WHERE pair_id = pairs.id ORDER BY time DESC LIMIT 1) AS close_price
                FROM pairs
                    INNER JOIN assets ON assets.id = pairs.primary_asset_id
                    INNER JOIN assets sassets ON sassets.id = pairs.secondary_asset_id`);
                for (let i = 0; i < stableQuoted.length; i++) {
                    const item = stableQuoted[i];
                    const asset = new AssetId(new Uint8Array(item['hash']));
                    const symbol = symbolOf(asset);
                    const quoteSymbol = symbolOf(new AssetId(new Uint8Array(item['quote_hash'])));
                    const quotePrice = results[quoteSymbol]?.price?.close;
                    const close = Common.bn(item['close_price']);
                    if (quotePrice == null || !quotePrice.gt(0) || close == null || !close.gt(0) || symbol == quoteSymbol)
                        continue;
                    const prev = results[symbol];
                    if (prev?.price?.close != null)
                        continue;
                    results[symbol] = {
                        whitelist: prev?.whitelist ?? Whitelist.has(asset),
                        base: prev?.base ?? null,
                        price: {
                            open: prev?.price?.open ?? close.times(quotePrice),
                            close: close.times(quotePrice)
                        }
                    };
                }
            } catch { }
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
	        (SELECT price FROM trades WHERE pair_id = tpair.id ORDER BY time DESC LIMIT 1) AS tertiary_price,
            passet.hash AS primary_asset,
            sasset.hash AS secondary_asset
        FROM assets passet
            INNER JOIN assets sasset ON sasset.id = ${secondaryAssetId.toString()}
            LEFT JOIN pairs ppair ON ppair.primary_asset_id = passet.id AND ppair.secondary_asset_id IS NULL
            LEFT JOIN pairs spair ON spair.primary_asset_id = sasset.id AND ppair.secondary_asset_id IS NULL
	        LEFT JOIN pairs tpair ON tpair.primary_asset_id = passet.id AND tpair.secondary_asset_id = sasset.id
        WHERE passet.id = ${primaryAssetId.toString()}`);
        if (!result.length)
            throw new Error('Pair not found');

        const primaryAsset = new AssetId(new Uint8Array(result[0]['primary_asset']));
        const primaryStable = Quotes.baseOf(symbolOf(primaryAsset)) != null;
        const primaryPrice = Common.bn(result[0]['primary_price']) || (primaryStable ? new BigNumber(1) : new BigNumber(NaN));
        const secondaryAsset = new AssetId(new Uint8Array(result[0]['secondary_asset']));
        const secondaryStable = Quotes.baseOf(symbolOf(secondaryAsset)) != null;
        const secondaryPrice = Common.bn(result[0]['secondary_price']) || (secondaryStable ? new BigNumber(1) : new BigNumber(NaN));
        const tertiaryPrice = Common.bn(result[0]['tertiary_price']) || (secondaryStable ? new BigNumber(1) : new BigNumber(NaN));
        let price = primaryPrice.dividedBy(secondaryPrice);
        price = price.isFinite() ? price : tertiaryPrice;
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
	            (SELECT time FROM trades WHERE pair_id = tpair.id ORDER BY time DESC LIMIT 1) AS tertiary_time,
                ppair.id AS primary_pair,
                spair.id AS secondary_pair,
                tpair.id AS tertiary_pair,
                passet.hash AS primary_asset,
                sasset.hash AS secondary_asset
            FROM assets passet
                INNER JOIN assets sasset ON sasset.id = ${secondaryAssetId.toString()}
                LEFT JOIN pairs ppair ON ppair.primary_asset_id = passet.id AND ppair.secondary_asset_id IS NULL
                LEFT JOIN pairs spair ON spair.primary_asset_id = sasset.id AND ppair.secondary_asset_id IS NULL
	            LEFT JOIN pairs tpair ON tpair.primary_asset_id = passet.id AND tpair.secondary_asset_id = sasset.id
            WHERE passet.id = ${primaryAssetId.toString()}
        )
        SELECT
            (SELECT SUM(price * GREATEST(quantity, 0.000001)) / SUM(GREATEST(quantity, 0.000001)) FROM trades WHERE pair_id = points.primary_pair AND time >= points.primary_time - ${interval}) AS primary_price,
            (SELECT SUM(price * GREATEST(quantity, 0.000001)) / SUM(GREATEST(quantity, 0.000001)) FROM trades WHERE pair_id = points.secondary_pair AND time >= points.secondary_time - ${interval}) AS secondary_price,
            (SELECT SUM(price * GREATEST(quantity, 0.000001)) / SUM(GREATEST(quantity, 0.000001)) FROM trades WHERE pair_id = points.tertiary_pair AND time >= points.tertiary_time - ${interval}) AS tertiary_price,
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
        const tertiaryPrice = Common.bn(result[0]['tertiary_price']) || (secondaryStable ? new BigNumber(1) : new BigNumber(NaN));
        let price = primaryPrice.dividedBy(secondaryPrice);
        price = price.isFinite() ? price : tertiaryPrice;
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
    static async getSyncedAccountBalancesByAccountId(id: Uint256, connection?: pq.TransactionSql): Promise<Balance[] | null> {
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
            (SELECT TRUE FROM poly_assets WHERE poly_assets.asset_id = weights.asset_id OR poly_assets.poly_asset_id = weights.asset_id LIMIT 1) AS poly_asset,
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
                    poly: balance['poly_asset'] || false,
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
    static async setSyncedAccountTierByAccountIdAndAsset(accountId: Uint256, assetId: Uint256, volume: BigNumber, makerFee: BigNumber, takerFee: BigNumber, connection?: pq.TransactionSql): Promise<void> {
        const sql = connection || this.connection;
        const top = await this.getLatestBlock(undefined, connection);
        await this.resultOf(sql`
        INSERT INTO tiers
        (
            account_id,
            asset_id,
            block_number,
            volume,
            maker_fee,
            taker_fee
        )
        VALUES
        (
            ${accountId.toString()},
            ${assetId.toString()},
            ${top?.blockNumber || 1},
            ${volume.toString()},
            ${makerFee.toString()},
            ${takerFee.toString()}
        )
        ON CONFLICT (account_id, asset_id) DO UPDATE SET
            block_number = EXCLUDED.block_number,
            volume = EXCLUDED.volume,
            maker_fee = EXCLUDED.maker_fee,
            taker_fee = EXCLUDED.taker_fee`);
    }
    static async getSyncedAccountTierByAccountIdAndAsset(accountId: Uint256, assetId: Uint256, connection?: pq.TransactionSql): Promise<{ volume: BigNumber | null, makerFee: BigNumber | null, takerFee: BigNumber | null } | null> {
        const sql = connection || this.connection;
        const synced = await this.getAccountSyncByAccountId(accountId, connection);
        if (!synced)
            return null;

        const result = await this.resultOf(sql`SELECT * FROM tiers WHERE account_id = ${accountId.toString()} AND asset_id = ${assetId.toString()}`);
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
    static async setContract(contract: { type: ContractType, accountId: Uint256, deployerAccountId: Uint256 | null, blockNumber: number | null, parameters: ContractParameters }, connection?: pq.TransactionSql): Promise<Contract | null> {
        const sql = connection || this.connection;
        const result = await this.resultOf(sql`
        INSERT INTO contracts
        (
            type,
            account_id,
            deployer_account_id,
            block_number,
            parameters
        )
        VALUES
        (
            ${contract.type},
            ${contract.accountId.toString()},
            ${contract.deployerAccountId ? contract.deployerAccountId.toString() : null},
            ${contract.blockNumber},
            ${sql.json(JSON.parse(JSON.stringify(contract.parameters)))}
        )
        ON CONFLICT (type) DO UPDATE SET
            account_id = EXCLUDED.account_id,
            deployer_account_id = EXCLUDED.deployer_account_id,
            block_number = EXCLUDED.block_number,
            parameters = EXCLUDED.parameters
        RETURNING *`);
        try {
            return this.toContract(result[0]);
        } catch {
            return null;
        }
    }
    static async setContractsSynced(connection?: pq.TransactionSql): Promise<void> {
        const sql = connection || this.connection;
        await this.resultOf(sql`UPDATE contracts SET synced = TRUE`);
    }
    static async getContractByType<T extends ContractParameters = ContractParameters>(type: ContractType, connection?: pq.TransactionSql): Promise<Contract<T> | null> {
        const sql = connection || this.connection;
        const result = await this.resultOf(sql`
        SELECT
            contracts.*,
            accounts.hash AS account_hash,
            deployer_accounts.hash AS deployer_account_hash
        FROM contracts
            INNER JOIN accounts ON accounts.id = contracts.account_id
            LEFT JOIN accounts deployer_accounts ON deployer_accounts.id = contracts.deployer_account_id
        WHERE contracts.type = ${type}`);
        try {
            return this.toContract(result[0]) as Contract<T>;
        } catch {
            return null;
        }
    }
    static async getContractByAccount<T extends ContractParameters = ContractParameters>(account: string, connection?: pq.TransactionSql): Promise<Contract<T> | null> {
        const sql = connection || this.connection;
        const accountId = await this.getAccountIdByAddress(account, false, connection);
        if (accountId == null)
            return null;

        const result = await this.resultOf(sql`
        SELECT
            contracts.*,
            accounts.hash AS account_hash,
            deployer_accounts.hash AS deployer_account_hash
        FROM contracts
            INNER JOIN accounts ON accounts.id = contracts.account_id
            LEFT JOIN accounts deployer_accounts ON deployer_accounts.id = contracts.deployer_account_id
        WHERE contracts.account_id = ${accountId.toString()}`);
        try {
            return this.toContract(result[0]) as Contract<T>;
        } catch {
            return null;
        }
    }
    static async getContracts(connection?: pq.TransactionSql): Promise<Contract[]> {
        const sql = connection || this.connection;
        const result = await this.resultOf(sql`
        SELECT
            contracts.*,
            accounts.hash AS account_hash,
            deployer_accounts.hash AS deployer_account_hash
        FROM contracts
            INNER JOIN accounts ON accounts.id = contracts.account_id
            LEFT JOIN accounts deployer_accounts ON deployer_accounts.id = contracts.deployer_account_id
        ORDER BY contracts.id`);
        try {
            let results: Contract[] = [];
            for (let i = 0; i < result.length; i++)
                results.push(this.toContract(result[i]));
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
        ON CONFLICT (order_id) DO UPDATE SET
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
            sasset.hash AS secondary_asset
        FROM orders
            INNER JOIN pairs ON pairs.id = pair_id
            INNER JOIN assets passet ON passet.id = pairs.primary_asset_id
            INNER JOIN assets sasset ON sasset.id = pairs.secondary_asset_id
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
            sasset.hash AS secondary_asset
        FROM orders
            INNER JOIN pairs ON pairs.id = pair_id
            INNER JOIN assets passet ON passet.id = pairs.primary_asset_id
            INNER JOIN assets sasset ON sasset.id = pairs.secondary_asset_id
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
            sasset.hash AS secondary_asset
        FROM orders
            INNER JOIN pairs ON pairs.id = pair_id
            INNER JOIN assets passet ON passet.id = pairs.primary_asset_id
            INNER JOIN assets sasset ON sasset.id = pairs.secondary_asset_id
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
    static async getOrdersByAccountIdAndPair(accountId: Uint256, pairId: Uint256, active: boolean | null, cursor: Cursor, connection?: pq.TransactionSql): Promise<Order[]> {
        const sql = connection || this.connection;
        const result = await this.resultOf(sql`
        SELECT
            orders.*,
            passet.hash AS primary_asset,
            sasset.hash AS secondary_asset
        FROM orders
            INNER JOIN pairs ON pairs.id = pair_id
            INNER JOIN assets passet ON passet.id = pairs.primary_asset_id
            INNER JOIN assets sasset ON sasset.id = pairs.secondary_asset_id
        WHERE orders.account_id = ${accountId.toString()} AND orders.pair_id = ${pairId.toString()}${typeof active == 'boolean' ? sql` AND active = ${active}` : sql``} ORDER BY active DESC, block_number DESC LIMIT ${cursor.count} OFFSET ${cursor.offset}`);
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
        ON CONFLICT (pool_id) DO UPDATE SET
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
            pools_view.volume
        FROM pools
            INNER JOIN pairs ON pairs.id = pair_id
            INNER JOIN assets passet ON passet.id = pairs.primary_asset_id
            INNER JOIN assets sasset ON sasset.id = pairs.secondary_asset_id
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
            pools_view.volume
        FROM pools
            INNER JOIN pairs ON pairs.id = pair_id
            INNER JOIN assets passet ON passet.id = pairs.primary_asset_id
            INNER JOIN assets sasset ON sasset.id = pairs.secondary_asset_id
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
            pools_view.volume
        FROM pools
            INNER JOIN pairs ON pairs.id = pair_id
            INNER JOIN assets passet ON passet.id = pairs.primary_asset_id
            INNER JOIN assets sasset ON sasset.id = pairs.secondary_asset_id
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
                        pair_id,
                        SUM(initial_primary_value) AS initial_primary_value,
                        SUM(initial_secondary_value) AS initial_secondary_value,
                        SUM(primary_value) AS primary_value,
                        SUM(secondary_value) AS secondary_value
                    FROM delegated_pools WHERE active = TRUE
                    GROUP BY pair_id
                )
                SELECT
                    sources.*,
                    delegators.account_id,
                    passet.hash AS primary_asset,
                    sasset.hash AS secondary_asset,
                    delegator_account.hash AS delegator_account_hash,
                    (SELECT v.fee_rate FROM pools v WHERE v.pair_id = sources.pair_id AND v.account_id = delegators.account_id AND v.active = TRUE LIMIT 1) AS fee_rate,
                    COALESCE((SELECT price FROM trades WHERE pair_id = (SELECT id FROM pairs WHERE primary_asset_id = passet.id AND secondary_asset_id IS NULL) ORDER BY time DESC LIMIT 1), 1) AS primary_price,
                    COALESCE((SELECT price FROM trades WHERE pair_id = (SELECT id FROM pairs WHERE primary_asset_id = sasset.id AND secondary_asset_id IS NULL) ORDER BY time DESC LIMIT 1), 1) AS secondary_price
                FROM sources
                    INNER JOIN pairs ON pairs.id = pair_id
                    INNER JOIN assets passet ON passet.id = pairs.primary_asset_id
                    INNER JOIN assets sasset ON sasset.id = pairs.secondary_asset_id
                    INNER JOIN contracts delegators ON delegators.type = ${ContractType.DLP}
                    INNER JOIN accounts delegator_account ON delegator_account.id = delegators.account_id
            )
            SELECT
                targets.*,
                initial_primary_value * primary_price + initial_secondary_value * secondary_price AS initial_value,
                primary_value * primary_price + secondary_value * secondary_price AS current_value,
                delegators_view.volume
            FROM targets
                LEFT JOIN delegators_view ON delegators_view.pair_id = targets.pair_id
            WHERE initial_primary_value > 0 OR initial_secondary_value > 0
        )
        SELECT * FROM aggregates ORDER BY 100000000 * (current_value - initial_value) / initial_value DESC`);
        try {
            const delegatedPools: PseudoDelegatedPool[] = [];
            for (let i = 0; i < result.length; i++) {
                const value = result[i];
                delegatedPools.push({
                    pairId: new Uint256(value['pair_id']),
                    delegatorAccount: value['delegator_account_hash'] ? Signing.encodeAddress(new Pubkeyhash(new Uint8Array(value['delegator_account_hash']))) || undefined : undefined,
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
            pools_view.volume
        FROM pools
            INNER JOIN pairs ON pairs.id = pair_id
            INNER JOIN assets passet ON passet.id = pairs.primary_asset_id
            INNER JOIN assets sasset ON sasset.id = pairs.secondary_asset_id
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
    static async getPoolsByAccountIdAndPair(accountId: Uint256, pairId: Uint256, active: boolean | null, cursor: Cursor, connection?: pq.TransactionSql): Promise<Pool[]> {
        const sql = connection || this.connection;
        const result = await this.resultOf(sql`
        SELECT
            pools.*,
            passet.hash AS primary_asset,
            sasset.hash AS secondary_asset,
            pools_view.volume
        FROM pools
            INNER JOIN pairs ON pairs.id = pair_id
            INNER JOIN assets passet ON passet.id = pairs.primary_asset_id
            INNER JOIN assets sasset ON sasset.id = pairs.secondary_asset_id
            LEFT JOIN pools_view ON pools_view.id = pools.id
        WHERE pools.account_id = ${accountId.toString()} AND pools.pair_id = ${pairId.toString()}${typeof active == 'boolean' ? sql` AND active = ${active}` : sql``} ORDER BY active DESC, block_number DESC LIMIT ${cursor.count} OFFSET ${cursor.offset}`);
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
    static async setDelegatedPool(pool: Omit<DelegatedPool, 'id'>, reset: boolean, connection?: pq.TransactionSql): Promise<DelegatedPool | null> {
        const sql = connection || this.connection;
        const result = await this.resultOf(sql`
        INSERT INTO delegated_pools
        (
            pair_id,
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
            ${pool.accountId.toString()},
            ${pool.blockNumber},
            ${pool.initialPrimaryValue.toString()},
            ${pool.initialSecondaryValue.toString()},
            ${pool.primaryValue.toString()},
            ${pool.secondaryValue.toString()},
            ${pool.rewardValue.toString()},
            ${pool.active}
        )
        ON CONFLICT (pair_id, account_id) DO UPDATE SET
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
            delegator_account.hash AS delegator_account_hash,
            delegators_view.volume,
            (SELECT ARRAY[SUM(v.primary_value), SUM(v.secondary_value)] FROM delegated_pools v WHERE v.pair_id = delegated_pools.pair_id AND v.active = TRUE) AS virtual_pool,
            (SELECT ARRAY[v.primary_value + v.primary_revenue - v.initial_primary_value, v.secondary_value + v.secondary_revenue - v.initial_secondary_value, v.primary_value, v.secondary_value, v.fee_rate, v.initial_price] FROM pools v WHERE v.pair_id = delegated_pools.pair_id AND v.account_id = delegators.account_id AND v.active = TRUE LIMIT 1) AS shadow_pool
        FROM delegated_pools
            INNER JOIN pairs ON pairs.id = pair_id
            INNER JOIN assets passet ON passet.id = pairs.primary_asset_id
            INNER JOIN assets sasset ON sasset.id = pairs.secondary_asset_id
            INNER JOIN contracts delegators ON delegators.type = ${ContractType.DLP}
            INNER JOIN accounts delegator_account ON delegator_account.id = delegators.account_id
            LEFT JOIN delegators_view ON delegators_view.id = delegators.id AND delegators_view.pair_id = pairs.id
        WHERE delegated_pools.id = ${id.toString()}`);
        try {
            return this.toDelegatedPool(result[0]);
        } catch {
            return null;
        }
    }
    static async getDelegatedPoolByHandle(pairId: Uint256, accountId: Uint256, connection?: pq.TransactionSql): Promise<DelegatedPool | null> {
        const sql = connection || this.connection;
        const result = await this.resultOf(sql`
        SELECT
            delegated_pools.*,
            passet.hash AS primary_asset,
            sasset.hash AS secondary_asset,
            delegator_account.hash AS delegator_account_hash,
            delegators_view.volume,
            (SELECT ARRAY[SUM(v.primary_value), SUM(v.secondary_value)] FROM delegated_pools v WHERE v.pair_id = delegated_pools.pair_id AND v.active = TRUE) AS virtual_pool,
            (SELECT ARRAY[v.primary_value + v.primary_revenue - v.initial_primary_value, v.secondary_value + v.secondary_revenue - v.initial_secondary_value, v.primary_value, v.secondary_value, v.fee_rate, v.initial_price] FROM pools v WHERE v.pair_id = delegated_pools.pair_id AND v.account_id = delegators.account_id AND v.active = TRUE LIMIT 1) AS shadow_pool
        FROM delegated_pools
            INNER JOIN pairs ON pairs.id = pair_id
            INNER JOIN assets passet ON passet.id = pairs.primary_asset_id
            INNER JOIN assets sasset ON sasset.id = pairs.secondary_asset_id
            INNER JOIN contracts delegators ON delegators.type = ${ContractType.DLP}
            INNER JOIN accounts delegator_account ON delegator_account.id = delegators.account_id
            LEFT JOIN delegators_view ON delegators_view.id = delegators.id AND delegators_view.pair_id = pairs.id
        WHERE delegated_pools.pair_id = ${pairId.toString()} AND delegated_pools.account_id = ${accountId.toString()}`);
        try {
            return this.toDelegatedPool(result[0]);
        } catch {
            return null;
        }
    }
    static async getAllDelegatedPoolsByPair(pairId: Uint256, connection?: pq.TransactionSql): Promise<DelegatedPool[]> {
        const sql = connection || this.connection;
        const result = await this.resultOf(sql`SELECT * FROM delegated_pools WHERE pair_id = ${pairId.toString()}`);
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
    static async getDelegatedPoolsByAccountIdAndPair(accountId: Uint256, pairId: Uint256, active: boolean | null, cursor: Cursor, connection?: pq.TransactionSql): Promise<DelegatedPool[]> {
        const sql = connection || this.connection;
        const result = await this.resultOf(sql`
        SELECT
            delegated_pools.*,
            passet.hash AS primary_asset,
            sasset.hash AS secondary_asset,
            delegator_account.hash AS delegator_account_hash,
            delegators_view.volume,
            (SELECT ARRAY[SUM(v.primary_value), SUM(v.secondary_value)] FROM delegated_pools v WHERE v.pair_id = delegated_pools.pair_id AND v.active = TRUE) AS virtual_pool,
            (SELECT ARRAY[v.primary_value + v.primary_revenue - v.initial_primary_value, v.secondary_value + v.secondary_revenue - v.initial_secondary_value, v.primary_value, v.secondary_value, v.fee_rate, v.initial_price] FROM pools v WHERE v.pair_id = delegated_pools.pair_id AND v.account_id = delegators.account_id AND v.active = TRUE LIMIT 1) AS shadow_pool
        FROM delegated_pools
            INNER JOIN pairs ON pairs.id = pair_id
            INNER JOIN assets passet ON passet.id = pairs.primary_asset_id
            INNER JOIN assets sasset ON sasset.id = pairs.secondary_asset_id
            INNER JOIN contracts delegators ON delegators.type = ${ContractType.DLP}
            INNER JOIN accounts delegator_account ON delegator_account.id = delegators.account_id
            LEFT JOIN delegators_view ON delegators_view.id = delegators.id AND delegators_view.pair_id = pairs.id
        WHERE delegated_pools.account_id = ${accountId.toString()} AND delegated_pools.pair_id = ${pairId.toString()}${typeof active == 'boolean' ? sql` AND active = ${active}` : sql``} ORDER BY active DESC, delegated_pools.block_number DESC LIMIT ${cursor.count} OFFSET ${cursor.offset}`);
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
            delegator_account.hash AS delegator_account_hash,
            delegators_view.volume,
            (SELECT ARRAY[SUM(v.primary_value), SUM(v.secondary_value)] FROM delegated_pools v WHERE v.pair_id = delegated_pools.pair_id AND v.active = TRUE) AS virtual_pool,
            (SELECT ARRAY[v.primary_value + v.primary_revenue - v.initial_primary_value, v.secondary_value + v.secondary_revenue - v.initial_secondary_value, v.primary_value, v.secondary_value, v.fee_rate, v.initial_price] FROM pools v WHERE v.pair_id = delegated_pools.pair_id AND v.account_id = delegators.account_id AND v.active = TRUE LIMIT 1) AS shadow_pool
        FROM delegated_pools
            INNER JOIN pairs ON pairs.id = pair_id
            INNER JOIN assets passet ON passet.id = pairs.primary_asset_id
            INNER JOIN assets sasset ON sasset.id = pairs.secondary_asset_id
            INNER JOIN contracts delegators ON delegators.type = ${ContractType.DLP}
            INNER JOIN accounts delegator_account ON delegator_account.id = delegators.account_id
            LEFT JOIN delegators_view ON delegators_view.id = delegators.id AND delegators_view.pair_id = pairs.id
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
        const contract = await this.getContractByType<DlpParameters>(ContractType.DLP, connection);
        if (contract == null || contract.deployerAccountId == null || !contract.deployerAccountId.eq(accountId))
            return [];

        const delegatorAccount = contract.account || '';
        const delegations = await this.resultOf(sql`
        WITH sources AS (
            SELECT
                pair_id,
                SUM(primary_value) AS primary_value,
                SUM(secondary_value) AS secondary_value
            FROM delegated_pools
            WHERE active = TRUE GROUP BY pair_id
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
        WHERE account_id = ${contract.accountId.toString()} AND active = TRUE`);
        let results: PseudoDelegatedState[] = [];
        results = [...results, ...(Array.isArray(contract.parameters.permissions) ? contract.parameters.permissions.map((x) => {
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
        return results;
    }
    static async setDepth(depth: Omit<Depth, 'id'>, connection?: pq.TransactionSql): Promise<Depth | null> {
        const sql = connection || this.connection;
        const result = await this.resultOf(sql`
        INSERT INTO depths
        (
            pair_id,
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
    static async setOracleTrades(trades: { asset: AssetId, trade: Omit<Trade, 'id' | 'pairId'> }[], connection: pq.TransactionSql): Promise<void> {
        const base = Quotes.globalBase();
        for (let i = 0; i < trades.length; i++) {
            const trade = trades[i];
            const pairId = await this.getPairByAssetHashes(trade.asset, null, false, true, connection);
            if (!pairId)
                throw new Error(`${symbolOf(trade.asset)}: invalid pair id`);

            const result = await this.setTrade({ pairId: pairId, ...trade.trade }, connection);
            if (!result)
                throw new Error(`${symbolOf(trade.asset)}: invalid trade`);

            await this.notify(Notification.TradeUpdate, {
                args: {
                    primaryAsset: trade.asset,
                    secondaryAsset: null,
                    secondaryBase: base,
                    account: null,
                    side: result.side,
                    price: result.price,
                    quantity: result.quantity,
                }
            }, connection);
        }
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
        const bucket = cursor.interval > 0 && cursor.interval % 1000 == 0 && this.intervals.includes(cursor.interval / 1000) ? cursor.interval / 1000 : null;
        if (bucket == null)
            throw new Error('interval is not precomputed: ' + cursor.interval);

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
        const bucketMs = bucket * 1000;
        const pairIds = (pairings.secondary != null && pairings.tertiary != null ? [pairings.primary, pairings.secondary, pairings.tertiary] : [pairings.primary]).map(v => v.toString());
        const tailStart = Math.ceil((Date.now() - bucketMs * 2 - 120000) / bucketMs) * bucketMs;
        const trades: Record<string, any>[] = [];
        if (cursor.fromTime < tailStart) {
            trades.push(...await this.resultOf(sql`
            SELECT
                pair_id,
                timepoint,
                volume,
                open,
                low,
                high,
                close
            FROM ${sql.unsafe('trades_' + bucket + '_view')}
            WHERE pair_id = ANY(${pairIds}) AND timepoint BETWEEN ${cursor.fromTime} AND ${Math.min(cursor.toTime, tailStart - 1)}
            ORDER BY timepoint`));
        }
        if (cursor.toTime >= tailStart) {
            trades.push(...await this.resultOf(sql`
            SELECT
                pair_id,
                time_bucket(${bucketMs}, time) AS timepoint,
                SUM(quantity) AS volume,
                FIRST(price, time) AS open,
                MIN(price) AS low,
                MAX(price) AS high,
                LAST(price, time) AS close
            FROM trades
            WHERE pair_id = ANY(${pairIds}) AND time BETWEEN ${Math.max(cursor.fromTime, tailStart)} AND ${cursor.toTime}
            GROUP BY pair_id, timepoint
            ORDER BY timepoint`));
        }
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
    static async getAggregatedLogsByPair(pairId: Uint256, cursor: Cursor, connection?: pq.TransactionSql): Promise<AggregatedLog[]> {
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
            WHERE pair_id = ${pairId.toString()}
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
            WHERE pair_id = ${pairId.toString()} AND (maker_account.hash IS NOT NULL OR taker_account.hash IS NOT NULL)
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
    static async getAggregatedLevelsByPair(pairId: Uint256, levels: number, connection?: pq.TransactionSql): Promise<{ ask: AggregatedLevel[], bid: AggregatedLevel[] }> {
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
            FROM orders WHERE pair_id = ${pairId.toString()} AND side = ${OrderSide.Sell} AND active = TRUE AND last_price > 0 AND last_quantity > 0
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
            FROM pools WHERE pair_id = ${pairId.toString()} AND active = TRUE AND last_ask_price > 0 AND primary_value > 0 AND (max_price IS NULL OR last_ask_price <= max_price)
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
            FROM orders WHERE pair_id = ${pairId.toString()} AND side = ${OrderSide.Buy} AND active = TRUE AND last_price > 0 AND last_quantity > 0
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
            FROM pools WHERE pair_id = ${pairId.toString()} AND active = TRUE AND last_bid_price > 0 AND secondary_value > 0 AND (min_price IS NULL OR last_bid_price >= min_price)
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
    static async getAggregatedPolyAssetIdsByPair(pairId: Uint256, connection?: pq.TransactionSql): Promise<{ primary: AssetId[], secondary: AssetId[] }> {
        const sql = connection || this.connection;
        const result = await this.resultOf(sql`
        WITH poly_ids AS (
            SELECT
                COALESCE((SELECT poly_asset_id FROM poly_assets WHERE asset_id = pairs.primary_asset_id), (SELECT poly_asset_id FROM poly_assets WHERE poly_asset_id = pairs.primary_asset_id LIMIT 1)) AS primary_poly_asset_id,
		        COALESCE((SELECT poly_asset_id FROM poly_assets WHERE asset_id = pairs.secondary_asset_id), (SELECT poly_asset_id FROM poly_assets WHERE poly_asset_id = pairs.secondary_asset_id LIMIT 1)) AS secondary_poly_asset_id
	        FROM pairs WHERE pairs.id = ${pairId.toString()}
        )
        SELECT asset_id, (poly_asset_id = COALESCE(primary_poly_asset_id, 0)) AS as_primary FROM poly_assets
            INNER JOIN poly_ids ON TRUE
        WHERE poly_asset_id IN (SELECT unnest(ARRAY[primary_poly_asset_id, secondary_poly_asset_id]) FROM poly_ids)`);
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
    static async getAggregatedPolyAssetIdsByMarket(targetAssetId: Uint256, connection?: pq.TransactionSql): Promise<AssetId[]> {
        const sql = connection || this.connection;
        const polyResult = await this.resultOf(sql`SELECT poly_asset_id FROM poly_assets WHERE asset_id = ${targetAssetId.toString()} OR poly_asset_id = ${targetAssetId.toString()} LIMIT 1`);
        const polyAssetId = (polyResult.length > 0 ? Common.u256(polyResult[0]['poly_asset_id']) : targetAssetId) || targetAssetId;
        const result = await this.resultOf(sql`SELECT asset_id FROM poly_assets WHERE poly_asset_id = ${polyAssetId.toString()}`);
        const assets: AssetId[] = [];
        for (let i = 0; i < result.length; i++) {
            const assetId = Common.u256(result[i]['asset_id']) || new Uint256();
            const asset: AssetId | null = await this.getAssetHashById(assetId, connection);
            if (asset != null)
                assets.push(asset);
        }

        const polyAsset: AssetId | null = await this.getAssetHashById(polyAssetId, connection);
        if (polyAsset != null && result.length > 0)
            assets.push(polyAsset);
        return assets;
    }
    static async getRoutingAmountByPairId(pairId: Uint256, side: OrderSide, amount: [BigNumber, BigNumber], slippage: BigNumber, connection?: pq.TransactionSql): Promise<[BigNumber, BigNumber, boolean]> {
        const sql = connection || this.connection;
        const result = await this.resultOf(sql`
        WITH levels AS (
            (
                SELECT
                    0 AS kind,
                    last_price AS price,
                    last_quantity AS quantity,
                    NULL::NUMERIC(96, 18) AS primary_value,
                    NULL::NUMERIC(96, 18) AS secondary_value,
                    NULL::NUMERIC(96, 18) AS liquidity,
                    NULL::NUMERIC(96, 18) AS state_price,
                    NULL::NUMERIC(96, 18) AS min_price,
                    NULL::NUMERIC(96, 18) AS max_price,
                    NULL::NUMERIC(96, 18) AS fee_rate
                FROM orders WHERE pair_id = ${pairId.toString()} AND side = ${side == OrderSide.Buy ? OrderSide.Sell : OrderSide.Buy} AND active = TRUE AND last_price > 0 AND last_quantity > 0
            )
            UNION ALL
            (
                SELECT
                    1 AS kind,
                    NULL::NUMERIC(96, 18) AS price,
                    0 AS quantity,
                    primary_value,
                    secondary_value,
                    liquidity,
                    price AS state_price,
                    min_price,
                    max_price,
                    fee_rate
                FROM pools WHERE pair_id = ${pairId.toString()} AND active = TRUE AND price > 0 AND primary_value > 0 AND secondary_value > 0
            )
        ) SELECT kind, price, quantity, primary_value, secondary_value, liquidity, state_price, min_price, max_price, fee_rate FROM levels`);
        type RoutingPool = {
            primaryValue: BigNumber,
            secondaryValue: BigNumber,
            liquidity: BigNumber,
            price: BigNumber,
            minPrice: BigNumber,
            maxPrice: BigNumber,
            feeRate: BigNumber,
            concentrated: boolean
        };
        const segments: { price: BigNumber, orderQuantity?: BigNumber, pool?: RoutingPool }[] = [];
        const orderLevels = new Map<string, { price: BigNumber, orderQuantity: BigNumber }>();
        for (const row of result) {
            if (Number(row['kind']) == 0) {
                const price = Common.bn(row['price']);
                const quantity = Common.bn(row['quantity']);
                if (price == null || quantity == null || price.lte(0) || quantity.lte(0))
                    continue;

                const level = orderLevels.get(price.toString());
                if (level)
                    level.orderQuantity = level.orderQuantity.plus(quantity);
                else {
                    const created = { price: price, orderQuantity: quantity };
                    orderLevels.set(price.toString(), created);
                    segments.push(created);
                }
            } else {
                const primaryValue = Common.bn(row['primary_value']);
                const secondaryValue = Common.bn(row['secondary_value']);
                const liquidity = Common.bn(row['liquidity']);
                const statePrice = Common.bn(row['state_price']);
                const minPrice = Common.bn(row['min_price']);
                const maxPrice = Common.bn(row['max_price']);
                const feeRate = Common.bn(row['fee_rate']) || new BigNumber(0);
                if (primaryValue == null || secondaryValue == null || liquidity == null || statePrice == null || liquidity.lte(0) || primaryValue.lte(0) || secondaryValue.lte(0) || statePrice.lte(0))
                    continue;

                const concentrated = minPrice != null && maxPrice != null && minPrice.gt(0) && maxPrice.gt(0);
                if (concentrated && (statePrice.lt(minPrice!) || statePrice.gt(maxPrice!)))
                    continue;

                const levelPrice = side == OrderSide.Buy ? statePrice.multipliedBy(feeRate.plus(1)) : BigNumber.max(statePrice.multipliedBy(new BigNumber(1).minus(feeRate)), new BigNumber(0));
                segments.push({ price: levelPrice, pool: { primaryValue: primaryValue, secondaryValue: secondaryValue, liquidity: liquidity, price: statePrice, minPrice: concentrated ? minPrice!.sqrt() : new BigNumber(0), maxPrice: concentrated ? maxPrice!.sqrt() : new BigNumber(0), feeRate: feeRate, concentrated: concentrated } });
            }
        }

        segments.sort(side == OrderSide.Buy ? (a, b) => a.price.comparedTo(b.price) || 0 : (a, b) => b.price.comparedTo(a.price) || 0);
        const best = segments.length > 0 ? segments[0].price : null;
        const fill = (amountIn: BigNumber, feeFactor: BigNumber): [BigNumber, BigNumber] => {
            let remaining = new BigNumber(amountIn);
            let totalOut = new BigNumber(0);
            if (best == null || remaining.lte(0) || slippage.lt(0))
                return [totalOut, remaining];

            const slip = BigNumber.min(slippage, new BigNumber(0.999999));
            const bound = side == OrderSide.Buy ? best.multipliedBy(slip.plus(1)) : best.multipliedBy(new BigNumber(1).minus(slip));
            if (bound.lte(0))
                return [totalOut, remaining];

            const boundSqrt = bound.sqrt();
            for (const segment of segments) {
                if (remaining.lte(0))
                    break;
                if (side == OrderSide.Buy ? segment.price.gt(bound) : segment.price.lt(bound))
                    break;

                if (segment.orderQuantity != null) {
                    const quantity = side == OrderSide.Buy ? BigNumber.min(remaining, segment.price.multipliedBy(segment.orderQuantity)) : BigNumber.min(remaining, segment.orderQuantity);
                    if (quantity.lte(0))
                        continue;

                    remaining = remaining.minus(quantity);
                    totalOut = totalOut.plus((side == OrderSide.Buy ? quantity.dividedBy(segment.price) : quantity.multipliedBy(segment.price)).multipliedBy(feeFactor));
                    continue;
                }

                const pool = segment.pool!;
                const feeRate = pool.feeRate.gt(0) && pool.feeRate.lt(1) ? pool.feeRate : new BigNumber(0);
                if (side == OrderSide.Buy) {
                    const capacity = pool.concentrated ? BigNumber.min(pool.liquidity.multipliedBy(BigNumber.max(boundSqrt.minus(pool.minPrice), new BigNumber(0))).minus(pool.secondaryValue), pool.liquidity.multipliedBy(pool.maxPrice.minus(pool.minPrice)).minus(pool.secondaryValue)) : pool.liquidity.multipliedBy(bound).sqrt().minus(pool.secondaryValue);
                    if (capacity.lte(0))
                        continue;

                    const amountBought = BigNumber.min(remaining, capacity);
                    const amountInAfterFee = amountBought.multipliedBy(new BigNumber(1).minus(feeRate));
                    if (!amountInAfterFee.gt(0))
                        continue;

                    const secondaryValue = pool.secondaryValue.plus(amountInAfterFee);
                    let primaryValue: BigNumber;
                    if (pool.concentrated) {
                        const sqrtPrice = secondaryValue.plus(pool.liquidity.multipliedBy(pool.minPrice)).dividedBy(pool.liquidity);
                        if (!sqrtPrice.gt(0) || sqrtPrice.gt(pool.maxPrice))
                            continue;

                        primaryValue = pool.liquidity.multipliedBy(BigNumber.max(pool.maxPrice.minus(sqrtPrice), new BigNumber(0))).dividedBy(sqrtPrice.multipliedBy(pool.maxPrice));
                    } else {
                        primaryValue = pool.liquidity.dividedBy(secondaryValue);
                    }

                    const amountSold = pool.primaryValue.minus(primaryValue);
                    if (!amountSold.gt(0))
                        continue;

                    remaining = remaining.minus(amountBought);
                    totalOut = totalOut.plus(amountSold.multipliedBy(feeFactor));
                } else {
                    const capacity = pool.concentrated ? BigNumber.min(pool.liquidity.multipliedBy(BigNumber.max(pool.maxPrice.minus(boundSqrt), new BigNumber(0))).dividedBy(boundSqrt.multipliedBy(pool.maxPrice)).minus(pool.primaryValue), pool.liquidity.multipliedBy(pool.maxPrice.minus(pool.minPrice)).dividedBy(pool.minPrice.multipliedBy(pool.maxPrice)).minus(pool.primaryValue)) : pool.liquidity.dividedBy(bound).sqrt().minus(pool.primaryValue);
                    if (capacity.lte(0))
                        continue;

                    const amountSold = BigNumber.min(remaining, capacity);
                    const amountInAfterFee = amountSold.multipliedBy(new BigNumber(1).minus(feeRate));
                    if (!amountInAfterFee.gt(0))
                        continue;

                    const primaryValue = pool.primaryValue.plus(amountInAfterFee);
                    let secondaryValue: BigNumber;
                    if (pool.concentrated) {
                        const sqrtPrice = pool.liquidity.multipliedBy(pool.maxPrice).dividedBy(primaryValue.multipliedBy(pool.maxPrice).plus(pool.liquidity));
                        if (!sqrtPrice.gt(0) || sqrtPrice.lt(pool.minPrice))
                            continue;

                        secondaryValue = pool.liquidity.multipliedBy(BigNumber.max(sqrtPrice.minus(pool.minPrice), new BigNumber(0)));
                    } else {
                        secondaryValue = pool.liquidity.dividedBy(primaryValue);
                    }

                    const amountBought = pool.secondaryValue.minus(secondaryValue);
                    if (!amountBought.gt(0))
                        continue;

                    remaining = remaining.minus(amountSold);
                    totalOut = totalOut.plus(amountBought.multipliedBy(feeFactor));
                }
            }

            return [totalOut, remaining];
        };

        const minFill = fill(amount[0], new BigNumber(1).minus(slippage));
        const maxFill = fill(amount[1], new BigNumber(1));
        return [minFill[0], maxFill[0], minFill[1].gt(0) || maxFill[1].gt(0)];
    }
    static async getRoutingPathsByAssetIds(fromAssetId: Uint256, toAssetId: Uint256, amount: BigNumber, slippage: BigNumber, depth: number, connection?: pq.TransactionSql): Promise<RouterPath[]> {
        const allPaths: RouterPath[] = [];
        const fromPolyAsset = (await this.getPolyAssetId(fromAssetId)) || fromAssetId;
        const toPolyAsset = (await this.getPolyAssetId(toAssetId)) || toAssetId;
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
                const [minOut, maxOut, capped] = await this.getRoutingAmountByPairId(pair.id, side, amountIn, slippage, connection);
                if (minOut.gt(0) || maxOut.gt(0)) {
                    queue.push([
                        assetOut.id,
                        [minOut, maxOut],
                        new Set(set).add(assetOut.id.toCompactHex()),
                        [...path, { pair, side: side, input: { min: amountIn[0], max: amountIn[1] }, output: { min: minOut, max: maxOut }, capped: capped }],
                    ]);
                }
            }
        }
        return allPaths.sort((a, b) => (a.some((leg) => leg.capped) ? 1 : 0) - (b.some((leg) => leg.capped) ? 1 : 0) || b[b.length - 1].output.min.comparedTo(a[a.length - 1].output.min) || 0);
    }
    private static toBlock(value: pq.Row): Block {
        return {
            blockNumber: Common.num(value['block_number']) || 0,
            blockHash: Common.u256(value['block_hash']) || new Uint256(),
        }
    }
    private static parametersOf(type: string, value: unknown): ContractParameters {
        const parameters = typeof value == 'string' ? JSON.parse(value || '{}') : (value || { }) as Record<string, unknown>;
        switch (type) {
            case ContractType.DEX:
                return {
                    poolExitFee: Common.bn(parameters['poolExitFee']) || new BigNumber(0),
                    maxPoolFeeRate: Common.bn(parameters['maxPoolFeeRate']) || new BigNumber(0),
                    minMakerFee: Common.bn(parameters['minMakerFee']) || new BigNumber(0),
                    maxMakerFee: Common.bn(parameters['maxMakerFee']) || new BigNumber(0),
                    makerFeeExponent: Common.num(parameters['makerFeeExponent']) || 0,
                    minTakerFee: Common.bn(parameters['minTakerFee']) || new BigNumber(0),
                    maxTakerFee: Common.bn(parameters['maxTakerFee']) || new BigNumber(0),
                    takerFeeExponent: Common.num(parameters['takerFeeExponent']) || 0,
                    assetVolumeTarget: Common.bn(parameters['assetVolumeTarget']) || new BigNumber(0),
                    assetResetDays: Common.num(parameters['assetResetDays']) || 0,
                    accountResetDays: Common.num(parameters['accountResetDays']) || 0,
                    marketPolicy: (Common.num(parameters['marketPolicy']) || 0) as MarketPolicy
                };
            case ContractType.DLP:
                return {
                    rewardEmission: Common.bn(parameters['rewardEmission']) || new BigNumber(0),
                    rewardBalance: Common.bn(parameters['rewardBalance']) || new BigNumber(0),
                    permissions: Array.isArray(parameters['permissions']) ? parameters['permissions'].map((x: any) => ({
                        primaryAssetId: Common.u256(x.primaryAssetId) || new Uint256(0),
                        primaryAsset: String(x.primaryAsset ?? ''),
                        secondaryAssetId: Common.u256(x.secondaryAssetId) || new Uint256(0),
                        secondaryAsset: String(x.secondaryAsset ?? '')
                    })) : []
                };
            case ContractType.AMS:
                return {
                    dex: typeof parameters['dex'] == 'string' ? parameters['dex'] : undefined,
                    feeOverflow: Common.bn(parameters['feeOverflow']) || new BigNumber(0),
                    bookValidityBlocks: Common.num(parameters['bookValidityBlocks']) || 0,
                    depositValidityBlocks: Common.num(parameters['depositValidityBlocks']) || 0,
                    depositPrecision: Common.num(parameters['depositPrecision']) ?? 6,
                    sequencer: this.sequencer?.address ?? null
                };
            default:
                return { };
        }
    }
    private static toContract(value: pq.Row): Contract {
        return {
            id: new Uint256(value['id']),
            type: value['type'],
            accountId: new Uint256(value['account_id']),
            account: value['account_hash'] ? Signing.encodeAddress(new Pubkeyhash(new Uint8Array(value['account_hash']))) || undefined : undefined,
            deployerAccountId: value['deployer_account_id'] ? new Uint256(value['deployer_account_id']) : null,
            deployerAccount: value['deployer_account_hash'] ? Signing.encodeAddress(new Pubkeyhash(new Uint8Array(value['deployer_account_hash']))) || undefined : null,
            blockNumber: Common.num(value['block_number']) ?? null,
            parameters: this.parametersOf(value['type'], value['parameters']),
            synced: value['synced'] === true
        };
    }
    private static toOrder(value: pq.Row): Order {
        return {
            id: new Uint256(value['id']),
            orderId: new Uint256(value['order_id']),
            pairId: new Uint256(value['pair_id']),
            primaryAsset: value['primary_asset'] ? new AssetId(new Uint8Array(value['primary_asset'])).id : undefined,
            secondaryAsset: value['secondary_asset'] ? new AssetId(new Uint8Array(value['secondary_asset'])).id : undefined,
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
            poolId: Common.u256(value['pool_id']) || new Uint256(0),
            accountId: Common.u256(value['account_id']) || new Uint256(0),
            blockNumber: value['block_number'] || 0,
            price: new BigNumber(value['price']),
            quantity: new BigNumber(value['quantity']),
            time: new Date(value['time'])
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
            delegatorAccount: value['delegator_account_hash'] ? Signing.encodeAddress(new Pubkeyhash(new Uint8Array(value['delegator_account_hash']))) || undefined : undefined,
            pairId: new Uint256(value['pair_id']),
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
            allocationPrice: price,
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
    static async updateBookingStatus(bookingId: Uint256, to: string, connection?: pq.TransactionSql, options?: { force?: boolean }): Promise<Booking | null> {
        const sql = connection || this.connection;
        const booking = await this.getBookingById(bookingId, connection);
        if (booking == null)
            throw new Error('AMS booking not found: ' + bookingId.toString());

        if (to != booking.status && !canTransition(booking.status, to) && options?.force !== true)
            throw new Error(`illegal AMS booking transition (${booking.status} -> ${to})`);

        if (to != booking.status)
            Log.info(`ams booking ${bookingId.toString()}: ${booking.status} -> ${to}${!canTransition(booking.status, to) ? ' (forced)' : ''}`);

        await this.resultOf(sql`UPDATE bookings SET status = ${to} WHERE id = ${bookingId.toString()}`);

        await this.notify(Notification.BookingUpdate, {
            accounts: await this.getBookingAccounts(booking, connection),
            args: {
                asset: booking.asset.id,
                amount: booking.amount.toString()
            }
        }, connection);
        return await this.getBookingById(bookingId, connection);
    }
    static async expireBookings(blockNumber: number, connection?: pq.TransactionSql): Promise<void> {
        const sql = connection || this.connection;
        const rows = await this.resultOf(sql`
        SELECT id FROM bookings
        WHERE expires_block <= ${blockNumber}
            AND status IN ('awaiting_deposit', 'finalizing_deposit', 'awaiting_execution')
        ORDER BY id`);
        for (let i = 0; i < rows.length; i++)
            await this.updateBookingStatus(new Uint256(rows[i]['id']), BookingStatus.Expired, connection);
    }
    private static async rollbackBookings(blockNumber: number, connection?: pq.TransactionSql): Promise<void> {
        const sql = connection || this.connection;
        const rows = await this.resultOf(sql`
        SELECT bookings.id FROM bookings
        WHERE bookings.status IN ('finalizing_deposit', 'awaiting_execution')
            AND (bookings.deposit_block IS NULL OR bookings.deposit_block >= ${blockNumber})
        ORDER BY bookings.id`);
        for (let i = 0; i < rows.length; i++) {
            const bookingId = new Uint256(rows[i]['id']);
            await this.updateBookingStatus(bookingId, BookingStatus.AwaitingDeposit, connection);
            await this.resultOf(sql`
            UPDATE bookings SET
                deposit_hash = NULL,
                deposit_source = NULL,
                deposit_block = NULL,
                deposit_payer_account_id = NULL,
                deposit_preattested_time = NULL
            WHERE id = ${bookingId.toString()}`);
            Log.info(`ams reorg: booking ${bookingId.toString()} fell back to ${BookingStatus.AwaitingDeposit} (block: ${blockNumber})`);
        }
    }
    private static async getBookingAccounts(booking: Booking, connection?: pq.TransactionSql): Promise<string[]> {
        const accounts: string[] = [];
        const contract = await this.getAmsContract(connection);
        if (contract?.account != null)
            accounts.push(contract.account);

        if (booking.depositPayer != null)
            accounts.push(booking.depositPayer);

        return accounts;
    }
    static async getAmsContract(connection?: pq.TransactionSql): Promise<Contract<AmsParameters> | null> {
        return await this.getContractByType<AmsParameters>(ContractType.AMS, connection);
    }
    static async setBooking(asset: AssetId, booking: { amount: BigNumber, publicKey: string, expiresBlock: number, txHash: Uint256, blockNumber: number }, connection?: pq.TransactionSql): Promise<Booking | null> {
        const sql = connection || this.connection;
        const assetId = await this.getAssetIdByHash(asset, 'trusted', connection);
        if (assetId == null)
            throw new Error('cannot find booked asset ' + asset.id);

        const result = await this.resultOf(sql`
        INSERT INTO bookings
        (
            asset_id,
            amount,
            public_key,
            status,
            expires_block,
            tx_hash,
            block_number
        )
        VALUES
        (
            ${assetId.toString()},
            ${booking.amount.toString()},
            ${booking.publicKey},
            ${BookingStatus.AwaitingDeposit},
            ${booking.expiresBlock},
            ${booking.txHash.toUint8Array()},
            ${booking.blockNumber}
        )
        ON CONFLICT (tx_hash) DO NOTHING
        RETURNING id`);
        if (result.length < 1)
            return await this.getBookingByTxHash(booking.txHash, connection);

        const bookingId = new Uint256(result[0]['id']);
        await this.notify(Notification.BookingUpdate, {
            accounts: [(await this.getAmsContract(connection))?.account].filter((v): v is string => v != null),
            args: {
                asset: asset.id,
                amount: booking.amount.toString()
            }
        }, connection);
        return await this.getBookingById(bookingId, connection);
    }
    static async getBookingByTxHash(txHash: Uint256, connection?: pq.TransactionSql): Promise<Booking | null> {
        const sql = connection || this.connection;
        const result = await this.resultOf(sql`
        ${this.bookingSelect(sql)}
        WHERE bookings.tx_hash = ${txHash.toUint8Array()}`);
        try {
            return this.toBooking(result[0]);
        } catch {
            return null;
        }
    }
    static async getBookingById(id: Uint256, connection?: pq.TransactionSql): Promise<Booking | null> {
        const sql = connection || this.connection;
        const result = await this.resultOf(sql`
        ${this.bookingSelect(sql)}
        WHERE bookings.id = ${id.toString()}`);
        try {
            return this.toBooking(result[0]);
        } catch {
            return null;
        }
    }
    static async getBookingByHandle(assetId: Uint256, amount: BigNumber, statuses: string[] | null, connection?: pq.TransactionSql): Promise<Booking | null> {
        const sql = connection || this.connection;
        const result = await this.resultOf(sql`
        ${this.bookingSelect(sql)}
        WHERE bookings.asset_id = ${assetId.toString()}
            AND bookings.amount = ${amount.toString()}
            AND (${statuses == null ? null : 'set'}::text IS NULL OR bookings.status = ANY(${statuses ?? []}))
        ORDER BY CASE WHEN bookings.status IN ('finalized', 'expired') THEN 1 ELSE 0 END, bookings.id DESC
        LIMIT 1`);
        try {
            return this.toBooking(result[0]);
        } catch {
            return null;
        }
    }
    static async getBookingByDepositHash(depositHash: Uint256, connection?: pq.TransactionSql): Promise<Booking | null> {
        const sql = connection || this.connection;
        const result = await this.resultOf(sql`
        ${this.bookingSelect(sql)}
        WHERE bookings.deposit_hash = ${depositHash.toUint8Array()}`);
        try {
            return this.toBooking(result[0]);
        } catch {
            return null;
        }
    }

    static async getBookings(cursor: Cursor, publicKeys?: string[] | null, statuses?: string[] | null, connection?: pq.TransactionSql): Promise<Booking[]> {
        const sql = connection || this.connection;
        const result = await this.resultOf(sql`
        ${this.bookingSelect(sql)}
        WHERE (${publicKeys == null ? null : 'set'}::text IS NULL OR bookings.public_key = ANY(${publicKeys ?? []}))
            AND (${statuses == null ? null : 'set'}::text IS NULL OR bookings.status = ANY(${statuses ?? []}))
        ORDER BY bookings.id DESC
        LIMIT ${cursor.count} OFFSET ${cursor.offset}`);

        const list: Booking[] = [];
        for (const row of result) {
            try {
                list.push(this.toBooking(row));
            } catch { }
        }

        return list;
    }
    static async hasPendingBookings(connection?: pq.TransactionSql): Promise<boolean> {
        const sql = connection || this.connection;
        const result = await this.resultOf(sql`
        SELECT EXISTS (SELECT TRUE FROM bookings
            WHERE bookings.status = 'awaiting_deposit'
                AND bookings.deposit_hash IS NULL
                AND bookings.expires_block > (SELECT COALESCE(MAX(block_number), 0) FROM blocks)) AS pending`);
        return result[0]?.['pending'] === true;
    }
    static async getBookingByDepositId(depositId: string, connection?: pq.TransactionSql): Promise<Booking | null> {
        const sql = connection || this.connection;
        const normalized = depositId.toLowerCase().replace(/^0x/, '');
        const result = await this.resultOf(sql`
        ${this.bookingSelect(sql)}
        WHERE LOWER(CASE WHEN bookings.deposit_id LIKE '0x%' THEN SUBSTRING(bookings.deposit_id FROM 3) ELSE bookings.deposit_id END) = ${normalized}
        ORDER BY bookings.id DESC LIMIT 1`);
        try {
            return this.toBooking(result[0]);
        } catch {
            return null;
        }
    }
    static async setBookingDepositFacts(bookingId: Uint256, facts: { depositHash?: Uint256 | null, depositId?: string | null, source: string, blockNumber?: number | null, payerAccountId?: Uint256 | null, preattestedTime?: number | null }, connection?: pq.TransactionSql): Promise<void> {
        const sql = connection || this.connection;
        await this.resultOf(sql`
        UPDATE bookings SET
            deposit_hash = COALESCE(${facts.depositHash?.toUint8Array() ?? null}, deposit_hash),
            deposit_id = COALESCE(${facts.depositId ?? null}, deposit_id),
            deposit_source = ${facts.source},
            deposit_block = COALESCE(${facts.blockNumber ?? null}, deposit_block),
            deposit_payer_account_id = COALESCE(${facts.payerAccountId?.toString() ?? null}, deposit_payer_account_id),
            deposit_preattested_time = COALESCE(${facts.preattestedTime ?? null}, deposit_preattested_time)
        WHERE id = ${bookingId.toString()}`);
    }
    static async setBookingOutput(bookingId: Uint256, outAssetId: Uint256, outAmount: BigNumber, connection?: pq.TransactionSql): Promise<void> {
        const sql = connection || this.connection;
        await this.resultOf(sql`UPDATE bookings SET out_asset_id = ${outAssetId.toString()}, out_amount = ${outAmount.toString()} WHERE id = ${bookingId.toString()}`);
    }
    static async setBookingWithdrawal(bookingId: Uint256, withdrawHash: Uint256, bridgeHash: Uint256 | null | undefined, expiresBlock: number, clearPipeline: boolean, connection?: pq.TransactionSql): Promise<void> {
        const sql = connection || this.connection;
        await this.resultOf(sql`
        UPDATE bookings SET
            withdraw_hash = ${withdrawHash.toUint8Array()},
            bridge_hash = COALESCE(${bridgeHash?.toUint8Array() ?? null}, bridge_hash),
            expires_block = ${expiresBlock},
            broadcast_hash = CASE WHEN ${clearPipeline} THEN NULL ELSE broadcast_hash END,
            proof_transaction_id = CASE WHEN ${clearPipeline} THEN NULL ELSE proof_transaction_id END,
            relay_error = CASE WHEN ${clearPipeline} THEN NULL ELSE relay_error END,
            attestate_hash = CASE WHEN ${clearPipeline} THEN NULL ELSE attestate_hash END
        WHERE id = ${bookingId.toString()}`);
    }
    static async setBookingRouting(bookingId: Uint256, routingAddress: string, connection?: pq.TransactionSql): Promise<void> {
        const sql = connection || this.connection;
        await this.resultOf(sql`
        UPDATE bookings SET routing_address = ${routingAddress}
        WHERE id = ${bookingId.toString()}`);
    }
    static async setBookingBroadcastProof(bookingId: Uint256, broadcastHash: Uint256, proofTransactionId: string, connection?: pq.TransactionSql): Promise<void> {
        const sql = connection || this.connection;
        await this.resultOf(sql`
        UPDATE bookings SET broadcast_hash = ${broadcastHash.toUint8Array()}, proof_transaction_id = ${proofTransactionId}, relay_error = NULL
        WHERE id = ${bookingId.toString()}`);
    }
    static async setBookingBroadcastError(bookingId: Uint256, broadcastHash: Uint256, relayError: string, connection?: pq.TransactionSql): Promise<void> {
        const sql = connection || this.connection;
        await this.resultOf(sql`
        UPDATE bookings SET broadcast_hash = ${broadcastHash.toUint8Array()}, relay_error = ${relayError}
        WHERE id = ${bookingId.toString()}`);
    }
    static async setBookingAttestate(bookingId: Uint256, attestateHash: Uint256, connection?: pq.TransactionSql): Promise<void> {
        const sql = connection || this.connection;
        await this.resultOf(sql`UPDATE bookings SET attestate_hash = ${attestateHash.toUint8Array()} WHERE id = ${bookingId.toString()}`);
    }
    private static async getBookingsByProofTransactionId(proofTransactionId: string, connection?: pq.TransactionSql): Promise<Booking[]> {
        const sql = connection || this.connection;
        const result = await this.resultOf(sql`
        ${this.bookingSelect(sql)}
        WHERE bookings.proof_transaction_id = ${proofTransactionId}
        ORDER BY bookings.id`);
        const bookings: Booking[] = [];
        for (let i = 0; i < result.length; i++) {
            try {
                bookings.push(this.toBooking(result[i]));
            } catch { }
        }

        return bookings;
    }
    private static async getBookingsByWithdrawHash(withdrawHash: Uint256, statuses: string[] | null, connection?: pq.TransactionSql): Promise<Booking[]> {
        const sql = connection || this.connection;
        const result = await this.resultOf(sql`
        ${this.bookingSelect(sql)}
        WHERE bookings.withdraw_hash = ${withdrawHash.toUint8Array()}
            AND (${statuses == null ? null : 'set'}::text IS NULL OR bookings.status = ANY(${statuses ?? []}))
        ORDER BY bookings.id`);
        const bookings: Booking[] = [];
        for (let i = 0; i < result.length; i++) {
            try {
                bookings.push(this.toBooking(result[i]));
            } catch { }
        }

        return bookings;
    }
    private static async findSwapRefOutput(events: EventInfo[], txHash: string, connection?: pq.TransactionSql): Promise<{ outAssetId: Uint256, outAmount: BigNumber } | null> {
        for (let i = 0; i < events.length; i++) {
            const log = events[i];
            if (log.hash == txHash && log.event.type == Spot.AMS.Events.SwapRef && Array.isArray(log.event.args) && log.event.args.length >= 4) {
                const outAssetId = await this.getAssetIdByHash(new AssetId(log.event.args[2]), 'trusted', connection);
                const outAmount = Common.bn(log.event.args[3]);
                if (outAssetId != null && outAmount != null)
                    return { outAssetId: outAssetId, outAmount: outAmount };
            }
        }

        return null;
    }
    static async withExclusiveLock<T>(action: () => Promise<T>): Promise<T> {
        if (this.config == null)
            throw new Error('exchange is not configured');

        const sql = pq({ ...this.config, max: 1 });
        try {
            await this.resultOf(sql`SELECT pg_advisory_lock(hashtext('ams:sequencer'))`);
            return await action();
        } finally {
            try {
                await this.resultOf(sql`SELECT pg_advisory_unlock(hashtext('ams:sequencer'))`);
            } finally {
                await sql.end({ timeout: 5 });
            }
        }
    }
    private static bookingSelect(sql: pq.Sql | pq.TransactionSql) {
        return sql`
        SELECT bookings.*, assets.hash AS asset_hash, out_assets.hash AS out_asset_hash, payer_accounts.hash AS deposit_payer_hash
        FROM bookings
            INNER JOIN assets ON assets.id = bookings.asset_id
            LEFT JOIN assets out_assets ON out_assets.id = bookings.out_asset_id
            LEFT JOIN accounts payer_accounts ON payer_accounts.id = bookings.deposit_payer_account_id
        `;
    }
    private static toBooking(value: pq.Row): Booking {
        return {
            id: new Uint256(value['id']),
            asset: new AssetId(new Uint8Array(value['asset_hash'])),
            assetId: new Uint256(value['asset_id']),
            amount: Common.bn(value['amount']) || new BigNumber(0),
            publicKey: String(value['public_key']),
            status: String(value['status']),
            expiresBlock: Common.num(value['expires_block']) || 0,
            txHash: new Uint256(new Uint8Array(value['tx_hash'])).toHex(),
            blockNumber: Common.num(value['block_number']) || 0,
            depositId: value['deposit_id'] != null ? String(value['deposit_id']) : null,
            depositHash: value['deposit_hash'] != null ? new Uint256(new Uint8Array(value['deposit_hash'])).toHex() : null,
            depositStatus: value['deposit_hash'] != null ? 'confirmed' : (value['deposit_id'] != null ? 'pending' : null),
            depositBlock: Common.num(value['deposit_block']) ?? null,
            depositPayer: value['deposit_payer_hash'] != null ? Signing.encodeAddress(new Pubkeyhash(new Uint8Array(value['deposit_payer_hash']))) || null : null,
            withdrawHash: value['withdraw_hash'] != null ? new Uint256(new Uint8Array(value['withdraw_hash'])).toHex() : null,
            bridgeHash: value['bridge_hash'] != null ? new Uint256(new Uint8Array(value['bridge_hash'])).toHex() : null,
            outAsset: value['out_asset_hash'] != null ? new AssetId(new Uint8Array(value['out_asset_hash'])) : null,
            outAmount: Common.bn(value['out_amount']) ?? null,
            broadcastHash: value['broadcast_hash'] != null ? new Uint256(new Uint8Array(value['broadcast_hash'])).toHex() : null,
            proofTransactionId: value['proof_transaction_id'] != null ? String(value['proof_transaction_id']) : null,
            relayError: value['relay_error'] != null ? String(value['relay_error']) : null,
            routingAddress: value['routing_address'] != null ? String(value['routing_address']) : null,
            attestateHash: value['attestate_hash'] != null ? new Uint256(new Uint8Array(value['attestate_hash'])).toHex() : null
        };
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