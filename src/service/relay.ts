import { randomBytes } from 'crypto';
import { AssetId, ByteUtil, DEX, Readability, Signing, Uint256 } from 'tangentsdk';
import { Log } from '../logging';
import { Connection, Cursor, Notification, Exchange, PriceDescriptors, RouterPath, TimeCursor } from './exchange';
import { FastifyInstance } from 'fastify/types/instance';
import { Blockchain, BlockchainInfo } from './blockchain';
import { AggregatedMatch, AggregatedPair, Order, OrderSide, Pool, Market as MarketT } from '../types';
import fastify, { FastifyReply, FastifyRequest } from 'fastify';
import fastifyWebsocket, { WebSocket } from '@fastify/websocket';
import cors from '@fastify/cors';
import BigNumber from 'bignumber.js';

function variable(data: any): string {
    try {
        const size = 48;
        const text = JSON.stringify(data);
        return text.length > size ? text.substring(0, size) + '...' : text;
    } catch {
        return '(unserializable)';
    }
}
function setHasAnyOf(set: Set<string>, containsOf: string[]): boolean {
    for (let i = 0; i < containsOf.length; i++) {
        if (set.has(containsOf[i]))
            return true;
    }
    return false;
}

export type Options = {
    host: string,
    port: number
};

export type ChannelNode = {
    id: string;
    socket: WebSocket,
    accounts: Set<string>
}

export type ChannelQuery = {
    channelId?: string;
    channelAccounts?: string[];
}

export type Balance = {
    asset: AssetId,
    unavailable: BigNumber,
    available: BigNumber,
    price: BigNumber | null
};

export class Result {
    static data(result: any, id?: string) {
        return { id: id || undefined, error: null, result: result };
    }
    static notification(result: any, id?: string) {
        return { id: id || undefined, error: null, notification: result };
    }
    static error(error: any, id?: string) {
        return { id: id || undefined, error: error && error.message ? error.message : null, result: null };
    }
}

export class Relay {
    static channels: Record<string, ChannelNode> = { };
    static server: any;

    static async setup(config: Options): Promise<void> {
        const server = fastify({
            trustProxy: true
        });
        server.register(cors, {
            origin: '*',
            methods: ['GET', 'POST']
        });
        server.register(fastifyWebsocket, {
            options: {
                maxPayload: 16777216,
                perMessageDeflate: true
            }
        });
        server.register(async (server) => {
            this.bindings(server);
            for (const channel in Notification) {
                const type = (Notification as any)[channel];
                await Exchange.listen(type, (notification) => this.notify(type as Notification, notification.query, notification.args));
            }
        });
        await server.listen({
            host: config.host,
            port: config.port
        });
    }
    static async shutdown(): Promise<void> {
        if (this.server) {
            await this.server.close();
            this.server = null;
        }
    }
    static bindings(server: FastifyInstance): void {
        new Router.Account(server);
        new Router.Asset(server);
        new Router.Market(server);
        new Router.Channel(server);
    }
    static callable(context: any, callback: (args?: any, response?: FastifyReply) => Promise<any>, asIs?: boolean): ((request: FastifyRequest, reply: FastifyReply) => Promise<any>) {
        return async (request: FastifyRequest, reply: FastifyReply): Promise<any> => {
            const args: Record<string, any> | any[] = typeof request.body != 'object' || request.body == null ? { } : request.body;
            if (!Array.isArray(args)) {
                for (let k in request.params as any) {
                    const v = (request.params as any)[k];
                    try {
                        args[k as string] = JSON.parse(v);
                    } catch {
                        args[k as string] = v;
                    }
                }
                for (let k in request.query as any) {
                    const v = (request.query as any)[k];
                    try {
                        args[k as string] = JSON.parse(v);
                    } catch {
                        args[k as string] = v;
                    }
                }
            }

            try {
                const result = await callback.apply(context, [args, reply]);
                const response = asIs ? result : Result.data(result);
                Log.info('relay', `${request.ip}${request.url} call:`, variable(args), '=>', variable(response));
                return response;
            } catch (exception: any) {
                const response = Result.error(exception);
                Log.error('relay', `${request.ip}${request.url} call:`, variable(args), '=>', variable(response));
                return response;
            }
        };
    }
    static channelable(context: any, callback: (channel: ChannelNode, args?: any) => Promise<any>): ((socket: WebSocket) => void) {
        return (socket: WebSocket) => {
            const id = ByteUtil.uint8ArrayToHexString(randomBytes(16));
            const channel = { id: id, socket: socket, accounts: new Set<string>() };
            this.channels[id] = channel;
            socket.on('close', () => delete this.channels[id]);
            socket.on('message', async (message: Buffer) => {
                try {
                    const body = JSON.parse(message.toString());
                    const args = typeof body != 'object' ? { } : body;
                    const url = args?.method || 'get://';
                    try {
                        const result = await callback.apply(context, [channel, args]);
                        socket.send(JSON.stringify(result));
                        Log.info('relay', `${id}/${url} call:`, variable(args), '=>', variable(result));
                    } catch (exception: any) {
                        socket.send(JSON.stringify(Result.error(exception, id)));
                        Log.error('relay', `${id}/${url} call:`, variable(args), '=>', exception);
                    }
                } catch (exception: any) {
                    socket.send(JSON.stringify(Result.error(exception, id)));
                    Log.error('relay', `${id}/null://err call:`, null, '=>', exception);
                }
            });
        };
    }
    static notify(type: Notification, query: ChannelQuery, args: Record<string, any>): number {
        let notifications = 0;
        let data = { type: type, data: args };
        if (query.channelId == null) {
            for (let id in this.channels) {
                try {
                    const channel = this.channels[id];
                    if (!query.channelAccounts || setHasAnyOf(channel.accounts, query.channelAccounts)) {
                        channel.socket.send(JSON.stringify({ id: id, error: null, notification: data }));
                        ++notifications;
                    }
                } catch { }
            }
        } else {
            const channel = this.channels[query.channelId];
            if (channel != null && (!query.channelAccounts || setHasAnyOf(channel.accounts, query.channelAccounts))) {
                channel.socket.send(JSON.stringify({ id: query.channelId, error: null, notification: data }));
                ++notifications;
            }
        }
        return notifications;
    }
}

export namespace Router {
    export type PageQuery = {
        page?: number;
    }

    export type AccountQuery = {
        id?: string | number;
        account?: string;
        resync?: boolean;
    }

    export class Asset {
        constructor(server: FastifyInstance) {
            Channel.register(server, 'get', '/assets/portfolio', Asset, Asset.getPortfolio);
            Channel.register(server, 'get', '/asset/prices', Asset, Asset.getPrices);
            Channel.register(server, 'get', '/asset/descriptors', Asset, Asset.getDescriptors);
            Channel.register(server, 'get', '/asset/query', Asset, Asset.getQuery);
            Channel.register(server, 'get', '/asset', Asset, Asset.get);
        }
        static async getPortfolio(): Promise<{ prices: PriceDescriptors, descriptors: BlockchainInfo[], markets: Market[] }> {
            const [prices, descriptors, markets] = await Promise.all([this.getPrices(), this.getDescriptors(), Exchange.getMarkets()]);
            return {
                prices: prices,
                descriptors: descriptors,
                markets: markets
            };
        }
        static async getPrices(): Promise<PriceDescriptors> {
            return await Exchange.getAssetPrices();
        }
        static async getDescriptors(): Promise<BlockchainInfo[]> {
            return await Blockchain.getBlockchains();
        }
        static async getQuery(args: { query?: string }): Promise<AssetId[]> {
            if (!args.query)
                throw new Error('Field \'query\' is required');

            return await Exchange.getAssetsByQuery(args.query);
        }
        static async get(args: { id?: string | number, handle?: string | number, chain?: string, token?: string, contractAddress?: string }): Promise<{ id: number, asset: AssetId }> {
            if (typeof args.id == 'string' || typeof args.id == 'number') {
                const id = new Uint256(args.id);
                const result = await Exchange.getAssetHashById(id);
                if (result != null)
                    return { id: id.toInteger(), asset: result };
            } else if (typeof args.handle == 'string' || typeof args.handle == 'number') {
                const asset = new AssetId(args.handle);
                const result = await Exchange.getAssetIdByHash(asset, false);
                if (result != null)
                    return { id: result.toInteger(), asset: asset };
            } else if (typeof args.chain == 'string') {
                const asset = AssetId.fromHandle(args.chain, args.token, args.contractAddress);
                const result = await Exchange.getAssetIdByHash(asset, true);
                if (result != null)
                    return { id: result.toInteger(), asset: asset };
            }
            throw new Error('Asset not found');
        }
    }

    export class Market {
        constructor(server: FastifyInstance) {
            Channel.register(server, 'get', '/market', Asset, Market.get);
            Channel.register(server, 'get', '/market/order', Asset, Market.getOrder);
            Channel.register(server, 'get', '/market/pool', Asset, Market.getPool);
            Channel.register(server, 'get', '/market/paths', Asset, Market.getPaths);
            Channel.register(server, 'get', '/market/assets', Asset, Market.getPolyAssets);
            Channel.register(server, 'get', '/market/pair', Asset, Market.getPair);
            Channel.register(server, 'get', '/market/pair/assets', Asset, Market.getPairPolyAssets);
            Channel.register(server, 'get', '/market/pairs', Asset, Market.getPairs);
            Channel.register(server, 'get', '/market/pair/trades', Asset, Market.getPairTrades);
            Channel.register(server, 'get', '/market/pair/price/series', Asset, Market.getPairPriceSeries);
            Channel.register(server, 'get', '/market/pair/price/levels', Asset, Market.getPairPriceLevels);
            Channel.register(server, 'get', '/markets', Asset, Market.getMarkets);
        }
        static async getMarkets() {
            return await Exchange.getMarkets();
        }
        static async getOrder(args: { id?: string | number }): Promise<Order> {
            if (typeof args.id != 'string' && typeof args.id != 'number')
                throw new Error('Order id is required');

            const orderId = new Uint256(args.id);
            const order = await Exchange.getOrderById(orderId);
            if (!order)
                throw new Error('Order not found');

            return order;
        }
        static async getPool(args: { id?: string | number }): Promise<Pool> {
            if (typeof args.id != 'string' && typeof args.id != 'number')
                throw new Error('Pool id is required');

            const poolId = new Uint256(args.id);
            const pool = await Exchange.getPoolById(poolId);
            if (!pool)
                throw new Error('Pool not found');

            return pool;
        }
        static async getPolyAssets(args: { assetHash?: string, liquidity?: boolean }): Promise<(AssetId & { liquidity?: BigNumber })[]> {
            if (typeof args.assetHash != 'string')
                throw new Error('Asset hash is required');

            const assetIdIn = await Exchange.getAssetIdByHash(new AssetId(args.assetHash), true);
            if (!assetIdIn)
                throw new Error('Asset hash in not found');

            const result: (AssetId & { marketId?: Uint256, liquidity?: BigNumber })[] = await Exchange.getAggregatedPolyAssetIdsByMarket(assetIdIn);
            if (args.liquidity) {
                const markets: Record<string, MarketT | null> = { };
                const accounts: Record<string, Balance[] | null> = { };
                for (let i = 0; i < result.length; i++) {
                    const asset = result[i];
                    let market = markets[asset.marketId?.toString() || ''];
                    market = (market || !asset.marketId ? market : (markets[asset.marketId?.toString() || ''] = await Exchange.getMarketById(asset.marketId)));
                    if (market != null && market.account != null) {
                        let balances = accounts[market.account];
                        balances = (balances ? balances : (accounts[market.account] = await Account.balancesOf({ id: market.accountId, address: market.account })))
                        asset.liquidity = balances?.find(x => x.asset.id == asset.id)?.available || new BigNumber(0);
                    }
                }
            }
            return result;
        }
        static async getPaths(args: { marketId?: string | number, assetHashIn?: string, assetHashOut?: string, amountIn?: BigNumber | string | number, slippage?: BigNumber | string | number }): Promise<RouterPath[]> {
            if (typeof args.marketId != 'string' && typeof args.marketId != 'number')
                throw new Error('Market id required');

            if (typeof args.assetHashIn != 'string')
                throw new Error('Asset hash in is required');

            if (typeof args.assetHashOut != 'string')
                throw new Error('Asset hash out is required');

            if (!args.amountIn)
                throw new Error('Amount in required');

            if (!args.slippage)
                throw new Error('Slippage required');

            const amountIn = new BigNumber(args.amountIn);
            if (!amountIn.gt(0))
                throw new Error('Amount must be positive');

            const slippage = new BigNumber(args.slippage);
            if (slippage.lt(0) || slippage.gt(1))
                throw new Error('Slippage must be between [0; 1]');

            const assetIdIn = await Exchange.getAssetIdByHash(new AssetId(args.assetHashIn), true);
            if (!assetIdIn)
                throw new Error('Asset hash in not found');

            const assetIdOut = await Exchange.getAssetIdByHash(new AssetId(args.assetHashOut), true);
            if (!assetIdOut)
                throw new Error('Asset hash out not found');
                
            const marketId = new Uint256(args.marketId);
            return await Exchange.getRoutingPathsByAssetIds(marketId, assetIdIn, assetIdOut, amountIn, slippage, 6);
        }
        static async getPair(args: { id?: string | number, primaryAssetHash?: string, secondaryAssetHash?: string, createIfNotExists?: boolean }): Promise<AggregatedPair> {
            if (typeof args.id != 'string' && typeof args.id != 'number')
                throw new Error('Market id or account is required');

            if (typeof args.primaryAssetHash != 'string')
                throw new Error('Primary asset hash is required');

            if (typeof args.secondaryAssetHash != 'string')
                throw new Error('Secondary asset hash is required');

            const marketId = new Uint256(args.id);
            const primaryAssetId = await Exchange.getAssetIdByHash(new AssetId(args.primaryAssetHash), true);
            const secondaryAssetId = await Exchange.getAssetIdByHash(new AssetId(args.secondaryAssetHash), true);
            const pairId = await Exchange.getPairByAssetIds(primaryAssetId, secondaryAssetId, marketId, !!args.createIfNotExists);
            if (!pairId)
                throw new Error('Pair is not valid');

            const symbols = await Exchange.getAggregatedPairs(marketId, pairId);
            if (!symbols.length)
                throw new Error('Symbol is not valid');

            return symbols[0];
        }
        static async getPairs(args: { id?: string | number }): Promise<AggregatedPair[]> {
            if (typeof args.id != 'string' && typeof args.id != 'number')
                throw new Error('Market id is required');

            const marketId = new Uint256(args.id);
            return await Exchange.getAggregatedPairs(marketId, null);
        }
        static async getPairPriceSeries(args: { pairId?: string | number, interval?: string | number, page?: string | number }, reply?: FastifyReply): Promise<[number, number, BigNumber, BigNumber, BigNumber, BigNumber, BigNumber][]> {
            const pairId = typeof args.pairId == 'string' || typeof args.pairId == 'number' ? new Uint256(args.pairId) : null;
            if (!pairId)
                throw new Error('Pair id is required');

            const interval = typeof args.interval == 'string' || typeof args.interval == 'number' ? 1000 * new Uint256(args.interval).toInteger() : null;
            if (!interval)
                throw new Error('interval is required');

            const page = typeof args.page == 'string' || typeof args.page == 'number' ? new Uint256(args.page).toInteger() : null;
            if (!page)
                throw new Error('page is required');

            const cursor = TimeCursor.page(interval, page);
            const result = await Exchange.getAggregatedTradesByPairId(pairId, cursor);
            const compressedResult = result.map((item) => [item.timepoint, item.side == OrderSide.Buy ? 1 : -1, item.volume, item.open, item.low, item.high, item.close]);
            if (reply != null && cursor.toTime < cursor.maxTime) {
                const expiration = new Date();
                expiration.setDate(expiration.getDate() + 90);
                reply.header('Cache-Control', 'public, max-age=7776000');
                reply.header('Expires', expiration.toUTCString());
            }
            return compressedResult as any;
        }
        static async getPairPriceLevels(args: { marketId?: string | number, pairId?: string | number, levels?: number }): Promise<{ ask: [number, BigNumber, BigNumber][], bid: [number, BigNumber, BigNumber][] }> {
            const marketId = typeof args.marketId == 'string' || typeof args.marketId == 'number' ? new Uint256(args.marketId) : null;
            if (!marketId)
                throw new Error('Market id is required');

            const pairId = typeof args.pairId == 'string' || typeof args.pairId == 'number' ? new Uint256(args.pairId) : null;
            if (!pairId)
                throw new Error('Pair id is required');

            const levels = typeof args.levels == 'number' ? args.levels : null;
            if (!levels || levels > 128)
                throw new Error('Levels must be within (0; 128]')

            const result = await Exchange.getAggregatedLevelsByMarketPair(marketId, pairId, levels);
            return {
                ask: result.ask.map((v) => [v.id.toInteger(), v.price, v.quantity]),
                bid: result.bid.map((v) => [v.id.toInteger(), v.price, v.quantity]),
            }
        }
        static async getPairTrades(args: { marketId?: string | number, pairId?: string | number } & PageQuery): Promise<AggregatedMatch[]> {
            const marketId = typeof args.marketId == 'string' || typeof args.marketId == 'number' ? new Uint256(args.marketId) : null;
            if (!marketId)
                throw new Error('Market id is required');

            const pairId = typeof args.pairId == 'string' || typeof args.pairId == 'number' ? new Uint256(args.pairId) : null;
            if (!pairId)
                throw new Error('Pair id is required');

            const result = await Exchange.getAggregatedMatchesByMarketPair(marketId, pairId, Cursor.page(args.page || 0));
            return result;
        }
        static async getPairPolyAssets(args: { marketId?: string | number, pairId?: string | number }): Promise<{ primary: AssetId[], secondary: AssetId[] }> {
            const marketId = typeof args.marketId == 'string' || typeof args.marketId == 'number' ? new Uint256(args.marketId) : null;
            if (!marketId)
                throw new Error('Market id is required');

            const pairId = typeof args.pairId == 'string' || typeof args.pairId == 'number' ? new Uint256(args.pairId) : null;
            if (!pairId)
                throw new Error('Pair id is required');

            const result = await Exchange.getAggregatedPolyAssetIdsByMarketPair(marketId, pairId);
            return result;
        }
        static async get(args: { id?: string | number, accountId?: string | number }): Promise<Market> {
            if (typeof args.id == 'string' || typeof args.id == 'number') {
                const id = new Uint256(args.id);
                const result = await Exchange.getMarketById(id);
                if (result != null)
                    return result;
            } else if (typeof args.accountId == 'string' || typeof args.accountId == 'number') {
                const id = new Uint256(args.accountId);
                const result = await Exchange.getMarketByAccountId(id);
                if (result != null)
                    return result;
            }
            throw new Error('Market not found');
        }
    }

    export class Account {
        constructor(server: FastifyInstance) {
            Channel.register(server, 'get', '/account', Account, Account.get);
            Channel.register(server, 'get', '/account/balances', Account, Account.getBalances);
            Channel.register(server, 'get', '/account/orders', Account, Account.getOrders);
            Channel.register(server, 'get', '/account/pools', Account, Account.getPools);
            Channel.register(server, 'get', '/account/tiers', Account, Account.getTiers);
        }
        static async getBalances(args: AccountQuery): Promise<{ asset: AssetId, unavailable: BigNumber, available: BigNumber, price: BigNumber | null }[]> {
            const account = await this.accountOf(args);
            if (!account)
                return [];

            const result = await this.balancesOf(account);
            return result;
        }
        static async getTiers(args: { marketId?: string | number, pairId?: string | number } & AccountQuery): Promise<{ primary: { volume: BigNumber | null, makerFee: BigNumber | null, takerFee: BigNumber | null }, secondary: { volume: BigNumber | null, makerFee: BigNumber | null, takerFee: BigNumber | null } }> {
            const marketId = typeof args.marketId == 'string' || typeof args.marketId == 'number' ? new Uint256(args.marketId) : null;
            if (!marketId)
                throw new Error('Market id is required');
            
            const pairId = typeof args.pairId == 'string' || typeof args.pairId == 'number' ? new Uint256(args.pairId) : null;
            if (!pairId)
                throw new Error('Pair id is required');

            const market = await Exchange.getMarketById(marketId);
            if (!market || !market.account)
                throw new Error('Not a valid market');

            const pair = await Exchange.getPairById(pairId);
            if (!pair || !pair.primaryAsset || !pair.secondaryAsset)
                throw new Error('Not a valid pair');

            const account = await this.accountOf(args);
            if (!account)
                throw new Error('Not a valid account');

            const fetchAccountTier = async (asset: AssetId, assetId: Uint256) => {
                const currentTier = await Blockchain.call(market.account || '', Readability.toFunction(DEX.Spot.accountAssetOf), [account.address, ['$uint256', asset.toHex()]]);
                await Exchange.setSyncedAccountTierByAccountIdAndMarketAsset(account.id, marketId, assetId, new BigNumber(currentTier.account?.volume?.toString() || ''), new BigNumber(currentTier.maker_fee?.toString() || ''), new BigNumber(currentTier.taker_fee?.toString() || ''));
            };

            let syncedPrimaryTier = await Exchange.getSyncedAccountTierByAccountIdAndMarketAsset(account.id, marketId, pair.primaryAsset.id);
            if (!syncedPrimaryTier) {
                await fetchAccountTier(pair.primaryAsset.hash, pair.primaryAsset.id);
                syncedPrimaryTier = await Exchange.getSyncedAccountTierByAccountIdAndMarketAsset(account.id, marketId, pair.primaryAsset.id);
            }
            
            let syncedSecondaryTier = await Exchange.getSyncedAccountTierByAccountIdAndMarketAsset(account.id, marketId, pair.secondaryAsset.id);
            if (!syncedSecondaryTier) {
                await fetchAccountTier(pair.secondaryAsset.hash, pair.secondaryAsset.id);
                syncedSecondaryTier = await Exchange.getSyncedAccountTierByAccountIdAndMarketAsset(account.id, marketId, pair.secondaryAsset.id);
            }
            
            return {
                primary: {
                    volume: syncedPrimaryTier?.volume || null,
                    makerFee: syncedPrimaryTier?.makerFee || null,
                    takerFee: syncedPrimaryTier?.takerFee || null
                },
                secondary: {
                    volume: syncedSecondaryTier?.volume || null,
                    makerFee: syncedSecondaryTier?.makerFee || null,
                    takerFee: syncedSecondaryTier?.takerFee || null
                }
            };
        }
        static async getOrders(args: { marketId?: string | number, pairId?: string | number, active?: boolean } & AccountQuery & PageQuery): Promise<Order[]> {
            const filter = typeof args.active == 'boolean' ? args.active : null;
            const account = await this.accountOf(args);
            if (!account)
                return [];

            const marketId = typeof args.marketId == 'string' || typeof args.marketId == 'number' ? new Uint256(args.marketId) : null;
            const pairId = typeof args.pairId == 'string' || typeof args.pairId == 'number' ? new Uint256(args.pairId) : null;
            if (marketId != null && pairId != null)
                return await Exchange.getOrdersByAccountIdAndMarketPair(account.id, marketId, pairId, filter, Cursor.page(args.page || 0));

            return await Exchange.getOrdersByAccountId(account.id, filter, Cursor.page(args.page || 0));
        }
        static async getPools(args: { marketId?: string | number, pairId?: string | number, active?: boolean } & AccountQuery & PageQuery): Promise<Pool[]> {
            const filter = typeof args.active == 'boolean' ? args.active : null;
            const account = await this.accountOf(args);
            if (!account)
                return [];

            const marketId = typeof args.marketId == 'string' || typeof args.marketId == 'number' ? new Uint256(args.marketId) : null;
            const pairId = typeof args.pairId == 'string' || typeof args.pairId == 'number' ? new Uint256(args.pairId) : null;
            if (marketId != null && pairId != null)
                return await Exchange.getPoolsByAccountIdAndMarketPair(account.id, marketId, pairId, filter, Cursor.page(args.page || 0));

            return await Exchange.getPoolsByAccountId(account.id, filter, Cursor.page(args.page || 0));
        }
        static async get(args: AccountQuery): Promise<{ id: number, account: string }> {
            const account = await this.accountOf(args);
            if (!account) 
                throw new Error('Account not found');
            
            return { id: account.id.toInteger(), account: account.address };
        }
        private static async accountOf(args: AccountQuery): Promise<{ id: Uint256, address: string, resync: boolean } | null> {
            if (typeof args.id == 'string' || typeof args.id == 'number') {
                const id = new Uint256(args.id);
                const result = await Exchange.getAccountHashById(id);
                if (result != null)
                    return { id: id, address: Signing.encodeAddress(result) || '', resync: args.resync || false };
            } else if (typeof args.account == 'string') {
                const account = args.account;
                const result = await Exchange.getAccountIdByAddress(account, false);
                if (result != null)
                    return { id: result, address: account, resync: args.resync || false };
            }
            return null;
        }
        static async balancesOf(account: { id: Uint256, address: string, resync?: boolean }): Promise<Balance[]> {
            let syncedBalances = account.resync ? null : await Exchange.getSyncedAccountBalancesByAccountId(account.id);
            if (!syncedBalances) {
                const currentBalances = (await Blockchain.getAccountBalances(account.address)).map((v) => { return { asset: new AssetId(v.asset), value: v.value }; });
                await Exchange.isolate(async (connection: Connection) => {
                    await Exchange.setSyncedAccountBalancesByAccountId(account.id, new Date(), currentBalances, connection);
                   syncedBalances = await Exchange.getSyncedAccountBalancesByAccountId(account.id, connection);
                });
            }
            return syncedBalances || [];
        }
    }

    export class Bot {
        constructor(server: FastifyInstance) {
            Channel.register(server, 'get', '/bot', Bot, Bot.get);
        }
        static async get(): Promise<{ }> {
            return { };
        }
    }

    export class Channel {
        static routes: Record<string, { context: any, callback: (args?: any, response?: FastifyReply) => Promise<any> }> = { };

        constructor(server: FastifyInstance) {
            server.get('/', { websocket: true }, Relay.channelable(Channel, Channel.message));
        }
        static async message(channel: ChannelNode, args: { id?: any, method: string, params: any }): Promise<any> {
            try {
                const method = typeof args.method == 'string' ? args.method : null;
                if (!method)
                    throw new Error('Illegal method');

                const params = args.params;
                if (!params || typeof params != 'object' || Array.isArray(params))
                    throw new Error('Illegal params');

                switch (method) {
                    case 'post://': {
                        const accounts = params.accounts;
                        if (!Array.isArray(accounts) || !accounts.every((v) => typeof v == 'string'))
                            break;

                        channel.accounts = new Set<string>(accounts);
                        return Result.data(channel.id, args.id);
                    }
                    default: {
                        const target = Channel.routes[method];
                        if (!target)
                            break;

                        const result = await target.callback.apply(target.context, [params]);
                        return Result.data(result, args.id);
                    }
                }

                throw new Error('Illegal operation'); 
            } catch (exception: any) {
                return Result.error(exception, args.id);
            } 
        }
        static register(server: FastifyInstance, method: 'get' | 'post', path: string, context: any, callback: (args?: any, response?: FastifyReply) => Promise<any>, asIs?: boolean): (args?: any, response?: FastifyReply) => Promise<any> {
            switch (method) {
                case 'get':
                    server.get(path, Relay.callable(context, callback, asIs));
                    break;
                case 'post':
                    server.post(path, Relay.callable(context, callback, asIs));
                    break;
                default:
                    throw new Error("Illegal method");
            }
            Channel.routes[method + ':/' + path] = {
                context: context,
                callback: callback
            };
            return callback;
        }
    }
}