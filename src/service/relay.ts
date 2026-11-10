import { randomBytes } from 'crypto';
import { AssetId, ByteUtil, Chain, Hashsig, Messages, RPC, SchemaUtil, Signing, Spot, Stream, Transactions, UiUtil, Uint256, Hashing } from 'tangentsdk';
import { Log } from '../logging';
import { Connection, Cursor, Notification, Exchange, PriceDescriptors, RouterPath, TimeCursor, NotificationData, Sequencer } from './exchange';
import { FastifyInstance } from 'fastify/types/instance';
import { Blockchain, BlockchainInfo } from './blockchain';
import { AggregatedLevel, AggregatedLog, AggregatedPair, Contract, ContractType, Order, Pool, DelegatedPool, PseudoDelegatedPool, PseudoDelegatedState, Balance } from '../types';
import fastify, { FastifyReply, FastifyRequest } from 'fastify';
import fastifyWebsocket, { WebSocket } from '@fastify/websocket';
import cors from '@fastify/cors';
import BigNumber from 'bignumber.js';
import { Peers } from './peers';
import { Quotes } from './market';
import { Common } from '../common';
import { Booking, BookingStatus, BookingSwap, AmsParameters, SpendMessageParams, RespendMessageParams, SendQuota, NoSendQuota } from '../types';
import { BookingAcquisition, BookingLimits, BookingRateLimit } from './ratelimit';

export type Options = {
    host: string,
    port: number,
    ipSource?: 'direct' | 'x-real-ip' | 'x-forwarded-for',
    rateLimits?: Partial<BookingLimits>
}

export type CallMeta = {
    ip: string | null
}

export type ChannelNode = {
    id: string;
    socket: WebSocket,
    accounts: Set<string>,
    ip: string
}

export type SwapExecution = {
    contractAccount: string,
    contractFunction: string,
    args: unknown[],
    describe: string,
    bridgeSend?: boolean
}

export type SwapReference = {
    transaction: string | null,
    amount?: string
}

export type BridgeSelection = {
    hash: string,
    chain: string,
    feeRate: string,
    supply: string | null,
    master: string | null,
    witnesses: string[],
    routingPolicy: string | null
}

export type BridgeQuote = BridgeSelection & {
    path: RouterPath[] | null,
    sendQuota: SendQuota,
    sendFee: string
}

class Result {
    static data(result: any, id?: string) {
        return { id: id || undefined, error: null, result: result };
    }
    static notification(result: any, id?: string) {
        return { id: id || undefined, error: null, notification: result };
    }
    static error(error: any, id?: string) {
        const retryAfter = error != null && typeof error.retryAfter == 'number' ? { retryAfter: error.retryAfter } : { };
        return { id: id || undefined, error: error && error.message ? error.message : null, result: null, ...retryAfter };
    }
}

export class Relay {
    static channels: Record<string, ChannelNode> = { };
    static server: any;
    static ipSource: 'direct' | 'x-real-ip' | 'x-forwarded-for' = 'direct';

    static resolveIp(request: FastifyRequest): string | null {
        if (this.ipSource == 'x-real-ip') {
            const header = request.headers['x-real-ip'];
            const value = Array.isArray(header) ? header[0] : header;
            if (typeof value == 'string' && value.trim().length > 0)
                return value.trim();
        } else if (this.ipSource == 'x-forwarded-for') {
            const header = request.headers['x-forwarded-for'];
            const value = Array.isArray(header) ? header[0] : header;
            if (typeof value == 'string' && value.length > 0) {
                const first = value.split(',')[0].trim();
                if (first.length > 0)
                    return first;
            }
        }
        return request.socket?.remoteAddress ?? request.ip ?? null;
    }
    static async setup(config: Options): Promise<void> {
        this.ipSource = config.ipSource || 'direct';
        BookingRateLimit.configure(config.rateLimits);
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
            server.get('/prices', { websocket: true }, (socket: WebSocket) => Peers.attach(socket));
            for (const channel in Notification) {
                const type = (Notification as any)[channel];
                await Exchange.listen(type, (notification) => this.notify(type as Notification, notification));
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
        new Router.Swaps(server);
        new Router.Channel(server);
    }
    static callable(context: any, callback: (args?: any, response?: FastifyReply, meta?: CallMeta) => Promise<any>, asIs?: boolean): ((request: FastifyRequest, reply: FastifyReply) => Promise<any>) {
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
                const result = await callback.apply(context, [args, reply, { ip: Relay.resolveIp(request) }]);
                const response = asIs ? result : Result.data(result);
                Log.info('relay', `${request.ip} ${request.method.toLowerCase()} ${request.url} call:`, Relay.variable(args), '=>', Relay.variable(response));
                return response;
            } catch (exception: any) {
                Log.error('relay', `${request.ip} ${request.method.toLowerCase()} ${request.url} call:`, Relay.variable(args), '=>', exception);
                return  Result.error(exception);
            }
        };
    }
    static channelable(context: any, callback: (channel: ChannelNode, args?: any) => Promise<any>): ((socket: WebSocket, request: FastifyRequest) => void) {
        return (socket: WebSocket, request: FastifyRequest) => {
            const id = ByteUtil.uint8ArrayToHexString(randomBytes(16));
            const ip = Relay.resolveIp(request) || id;
            const channel = { id: id, socket: socket, accounts: new Set<string>(), ip: ip };
            this.channels[id] = channel;
            socket.on('close', () => delete this.channels[id]);
            socket.on('message', async (message: Buffer) => {
                try {
                    const body = JSON.parse(message.toString());
                    const args = typeof body != 'object' ? { } : body;
                    const url = (args?.method || 'get null').replace('://', ' ');
                    try {
                        const result = await callback.apply(context, [channel, args]);
                        socket.send(JSON.stringify(result));
                        Log.info('relay', `${ip} ${url} call:`, Relay.variable(args), '=>', Relay.variable(result));
                    } catch (exception: any) {
                        socket.send(JSON.stringify(Result.error(exception, id)));
                        Log.error('relay', `${ip} ${url} call:`, Relay.variable(args), '=>', exception);
                    }
                } catch (exception: any) {
                    socket.send(JSON.stringify(Result.error(exception, id)));
                    Log.error('relay', `${ip} error call:`, null, '=>', exception);
                }
            });
        };
    }
    static notify(type: Notification, body: NotificationData): number {
        let notifications = 0;
        let data = { type: type, data: body.args };
        if (body.channelId == null) {
            for (let id in this.channels) {
                try {
                    const channel = this.channels[id];
                    if (!body.accounts || Relay.anyOf(channel.accounts, body.accounts)) {
                        channel.socket.send(JSON.stringify({ id: id, error: null, notification: data }));
                        ++notifications;
                    }
                } catch { }
            }
        } else {
            const channel = this.channels[body.channelId];
            if (channel != null && (!body.accounts || Relay.anyOf(channel.accounts, body.accounts))) {
                channel.socket.send(JSON.stringify({ id: body.channelId, error: null, notification: data }));
                ++notifications;
            }
        }
        return notifications;
    }
    private static variable(data: any): string {
        try {
            const size = 48;
            const text = JSON.stringify(data);
            return text.length > size ? text.substring(0, size) + '...' : text;
        } catch {
            return '(unserializable)';
        }
    }
    private static anyOf(set: Set<string>, containsOf: string[]): boolean {
        for (let i = 0; i < containsOf.length; i++) {
            if (set.has(containsOf[i]))
                return true;
        }
        return false;
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
            Channel.register(server, 'get', '/assets', Asset, Asset.getPortfolio);
            Channel.register(server, 'get', '/asset/prices', Asset, Asset.getPrices);
            Channel.register(server, 'get', '/asset/descriptors', Asset, Asset.getDescriptors);
            Channel.register(server, 'get', '/asset/query', Asset, Asset.getQuery);
            Channel.register(server, 'get', '/asset', Asset, Asset.get);
        }
        static async getPortfolio(args?: { base?: string }): Promise<{ prices: PriceDescriptors, descriptors: BlockchainInfo[], contracts: Contract[], tip: number | null, rate: BigNumber | null }> {
            const [prices, descriptors, contracts, tip, rate] = await Promise.all([this.getPrices(), this.getDescriptors(), Exchange.getContracts(), Exchange.getLatestBlock(), Market.rateOf(args?.base)]);
            return {
                prices: prices,
                descriptors: descriptors,
                contracts: contracts,
                tip: tip != null ? tip.blockNumber : null,
                rate: rate
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
                const result = await Exchange.getAssetIdByHash(asset, 'trusted');
                if (result != null)
                    return { id: result.toInteger(), asset: asset };
            } else if (typeof args.chain == 'string') {
                const asset = AssetId.fromHandle(args.chain, args.token, args.contractAddress);
                const result = await Exchange.getAssetIdByHash(asset, 'untrusted');
                if (result != null)
                    return { id: result.toInteger(), asset: asset };
            }
            throw new Error('Asset not found');
        }
    }

    export class Market {
        constructor(server: FastifyInstance) {
            Channel.register(server, 'get', '/market', Asset, Market.get);
            Channel.register(server, 'get', '/market/metrics', Asset, Market.getMetrics);
            Channel.register(server, 'get', '/market/order', Asset, Market.getOrder);
            Channel.register(server, 'get', '/market/pool', Asset, Market.getPool);
            Channel.register(server, 'get', '/market/pool/delegated', Asset, Market.getDelegatedPool);
            Channel.register(server, 'get', '/market/pools', Asset, Market.getPools);
            Channel.register(server, 'get', '/market/pools/delegated', Asset, Market.getDelegatedPools);
            Channel.register(server, 'get', '/market/paths', Asset, Market.getPaths);
            Channel.register(server, 'get', '/market/assets', Asset, Market.getPolyAssets);
            Channel.register(server, 'get', '/market/price', Asset, Market.getPrice);
            Channel.register(server, 'get', '/market/price/history', Asset, Market.getPriceHistory);
            Channel.register(server, 'get', '/market/pair', Asset, Market.getPair);
            Channel.register(server, 'get', '/market/pair/assets', Asset, Market.getPairPolyAssets);
            Channel.register(server, 'get', '/market/pairs', Asset, Market.getPairs);
            Channel.register(server, 'get', '/market/pair/logs', Asset, Market.getPairLogs);
            Channel.register(server, 'get', '/market/pair/price/series', Asset, Market.getPairPriceSeries);
            Channel.register(server, 'get', '/market/pair/price/levels', Asset, Market.getPairPriceLevels);
            Channel.register(server, 'get', '/contracts', Asset, Market.getContracts);
            Channel.register(server, 'get', '/contract', Asset, Market.getContract);
            Channel.register(server, 'get', '/market/rate', Asset, Market.getRate);
        }
        static async rateOf(base?: string): Promise<BigNumber | null> {
            if (typeof base != 'string' || base.length == 0)
                return new BigNumber(1);

            const globalBase = Quotes.globalBase();
            if (!globalBase)
                return null;

            try {
                return (await Quotes.currencyPriceOf(AssetId.fromHandle(globalBase), AssetId.fromHandle(base))).value;
            } catch {
                return null;
            }
        }
        static async getRate(args: { base?: string }): Promise<BigNumber | null> {
            if (typeof args.base != 'string' || args.base.length == 0)
                throw new Error('Base is required');

            return await Market.rateOf(args.base);
        }
        static async getMetrics(_: any, reply?: FastifyReply) {
            if (reply != null) {
                const expiration = new Date();
                expiration.setDate(expiration.getDate() + 1);
                reply.header('Cache-Control', 'public, max-age=86400');
                reply.header('Expires', expiration.toUTCString());
            }
            return await Exchange.getOverallMetrics();
        }
        static async getContracts(): Promise<Contract[]> {
            return await Exchange.getContracts();
        }
        static async getContract(args: { type?: string }): Promise<Contract> {
            if (typeof args.type != 'string' || args.type.length == 0)
                throw new Error('Contract type is required');

            const contract = await Exchange.getContractByType(args.type as ContractType);
            if (contract == null)
                throw new Error('Contract not found: ' + args.type);

            return contract;
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
        static async getDelegatedPool(args: { id?: string | number }): Promise<DelegatedPool> {
            if (typeof args.id != 'string' && typeof args.id != 'number')
                throw new Error('Delegated pool id is required');

            const poolId = new Uint256(args.id);
            const pool = await Exchange.getDelegatedPoolById(poolId);
            if (!pool)
                throw new Error('Delegated pool not found');

            return pool;
        }
        static async getPools(args: PageQuery): Promise<Pool[]> {
            return await Exchange.getBestPools(Cursor.page(args.page || 0));
        }
        static async getDelegatedPools(): Promise<PseudoDelegatedPool[]> {
            return await Exchange.getBestDelegatedPools();
        }
        static async getPolyAssets(args: { assetHash?: string, liquidity?: boolean }): Promise<(AssetId & { liquidity?: BigNumber })[]> {
            if (typeof args.assetHash != 'string')
                throw new Error('Asset hash is required');

            const assetIdIn = await Exchange.getAssetIdByHash(new AssetId(args.assetHash), 'untrusted');
            if (!assetIdIn)
                throw new Error('Asset hash in not found');

            const result: (AssetId & { liquidity?: BigNumber })[] = await Exchange.getAggregatedPolyAssetIdsByMarket(assetIdIn);
            if (args.liquidity) {
                const dex = await Exchange.getContractByType(ContractType.DEX);
                if (dex != null && dex.account != null) {
                    const balances = await Account.balancesOf({ id: dex.accountId, address: dex.account });
                    for (let i = 0; i < result.length; i++)
                        result[i].liquidity = balances.find(x => x.asset.id == result[i].id)?.available || new BigNumber(0);
                }
            }
            return result;
        }
        static async getPaths(args: { assetHashIn?: string, assetHashOut?: string, amountIn?: BigNumber | string | number, slippage?: BigNumber | string | number }): Promise<RouterPath[]> {
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

            const assetIdIn = await Exchange.getAssetIdByHash(new AssetId(args.assetHashIn), 'untrusted');
            if (!assetIdIn)
                throw new Error('Asset hash in not found');

            const assetIdOut = await Exchange.getAssetIdByHash(new AssetId(args.assetHashOut), 'untrusted');
            if (!assetIdOut)
                throw new Error('Asset hash out not found');

            return await Exchange.getRoutingPathsByAssetIds(assetIdIn, assetIdOut, amountIn, slippage, 6);
        }
        static async getPrice(args: { primaryAssetHash?: string, secondaryAssetHash?: string, interval?: BigNumber | string | number }): Promise<BigNumber | null> {
            if (typeof args.primaryAssetHash != 'string')
                throw new Error('Primary asset hash is required');

            if (typeof args.secondaryAssetHash != 'string')
                throw new Error('Secondary asset hash is required');

            const interval = typeof args.interval == 'string' || typeof args.interval == 'number' ? 1000 * new Uint256(args.interval).toInteger() : null;
            if (args.interval && !interval)
                throw new Error('interval is not valid');

            const primaryAssetId = await Exchange.getAssetIdByHash(new AssetId(args.primaryAssetHash), 'untrusted');
            if (!primaryAssetId)
                throw new Error('Primary asset id not found');

            const secondaryAssetId = await Exchange.getAssetIdByHash(new AssetId(args.secondaryAssetHash), 'untrusted');
            if (!secondaryAssetId)
                throw new Error('Secondary asset id not found');

            const price = interval ? await Exchange.getAssetTWAP(primaryAssetId, secondaryAssetId, interval) : await Exchange.getAssetPrice(primaryAssetId, secondaryAssetId);
            return price;
        }
        static async getPriceHistory(args: { assetHashes: string, interval: BigNumber | string | number, points: BigNumber | string | number }): Promise<Record<string, AssetId & { history: [number, BigNumber][] }>> {
            const assets = typeof args.assetHashes == 'string' ? args.assetHashes.split(',').map(v => new AssetId(v.toString())) : [];
            if (!Array.isArray(assets) || !assets.length)
                throw new Error('Asset hashes must be a non-empty array');

            const interval = typeof args.interval == 'string' || typeof args.interval == 'number' ? 1000 * new Uint256(args.interval).toInteger() : null;
            if (!interval || interval < 1)
                throw new Error('interval is not valid');

            const points = typeof args.points == 'string' || typeof args.points == 'number' ? new Uint256(args.points).toInteger() : null;
            if (!points || points <= 0 || points > 31)
                throw new Error('points is not valid');

            return await Exchange.getAssetPriceHistory(assets, interval, points);
        }
        static async getPair(args: { primaryAssetHash?: string, secondaryAssetHash?: string, createIfNotExists?: boolean }): Promise<AggregatedPair> {
            if (typeof args.primaryAssetHash != 'string')
                throw new Error('Primary asset hash is required');

            if (typeof args.secondaryAssetHash != 'string')
                throw new Error('Secondary asset hash is required');

            const primaryAssetId = await Exchange.getAssetIdByHash(new AssetId(args.primaryAssetHash), 'untrusted');
            const secondaryAssetId = await Exchange.getAssetIdByHash(new AssetId(args.secondaryAssetHash), 'untrusted');
            const pairId = await Exchange.getPairByAssetIds(primaryAssetId, secondaryAssetId, !!args.createIfNotExists);
            if (!pairId)
                throw new Error('Pair is not valid');

            const symbols = await Exchange.getAggregatedPairs(pairId);
            if (!symbols.length)
                throw new Error('Symbol is not valid');

            return symbols[0];
        }
        static async getPairs(): Promise<AggregatedPair[]> {
            return await Exchange.getAggregatedPairs(null);
        }
        static async getPairPriceSeries(args: { pairId?: string | number, interval?: string | number, page?: string | number }, reply?: FastifyReply): Promise<[number, BigNumber, BigNumber, BigNumber, BigNumber, BigNumber][]> {
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
            const compressedResult = result.map((item) => [item.timepoint, item.volume, item.open, item.low, item.high, item.close]);
            if (reply != null && cursor.toTime < cursor.maxTime) {
                const expiration = new Date();
                expiration.setDate(expiration.getDate() + 90);
                reply.header('Cache-Control', 'public, max-age=7776000');
                reply.header('Expires', expiration.toUTCString());
            }
            return compressedResult as any;
        }
        static async getPairPriceLevels(args: { pairId?: string | number, levels?: number }): Promise<{ ask: any[], bid: any[] }> {
            const pairId = typeof args.pairId == 'string' || typeof args.pairId == 'number' ? new Uint256(args.pairId) : null;
            if (!pairId)
                throw new Error('Pair id is required');

            const levels = typeof args.levels == 'number' ? args.levels : null;
            if (!levels || levels > 128)
                throw new Error('Levels must be within (0; 128]')

            const result = await Exchange.getAggregatedLevelsByPair(pairId, levels);
            return {
                ask: result.ask.map((v: AggregatedLevel) => v.curve ? [v.id.toInteger(), v.price, v.quantity, v.curve.minPrice, v.curve.maxPrice, v.curve.primaryValue, v.curve.secondaryValue, v.curve.feeRate] : [v.id.toInteger(), v.price, v.quantity]),
                bid: result.bid.map((v: AggregatedLevel) => v.curve ? [v.id.toInteger(), v.price, v.quantity, v.curve.minPrice, v.curve.maxPrice, v.curve.primaryValue, v.curve.secondaryValue, v.curve.feeRate] : [v.id.toInteger(), v.price, v.quantity]),
            }
        }
        static async getPairLogs(args: { pairId?: string | number } & PageQuery): Promise<AggregatedLog[]> {
            const pairId = typeof args.pairId == 'string' || typeof args.pairId == 'number' ? new Uint256(args.pairId) : null;
            if (!pairId)
                throw new Error('Pair id is required');

            const result = await Exchange.getAggregatedLogsByPair(pairId, Cursor.page(args.page || 0));
            return result;
        }
        static async getPairPolyAssets(args: { pairId?: string | number }): Promise<{ primary: AssetId[], secondary: AssetId[] }> {
            const pairId = typeof args.pairId == 'string' || typeof args.pairId == 'number' ? new Uint256(args.pairId) : null;
            if (!pairId)
                throw new Error('Pair id is required');
            return await Exchange.getAggregatedPolyAssetIdsByPair(pairId);
        }
        static async get(args: { id?: string | number, accountId?: string | number }): Promise<Contract> {
            const contracts = await Exchange.getContracts();
            if (typeof args.id == 'string' || typeof args.id == 'number') {
                const id = new Uint256(args.id);
                const result = contracts.find(v => v.id.eq(id));
                if (result != null)
                    return result;
            } else if (typeof args.accountId == 'string' || typeof args.accountId == 'number') {
                const id = new Uint256(args.accountId);
                const result = contracts.find(v => v.accountId.eq(id));
                if (result != null)
                    return result;
            }
            throw new Error('Contract not found');
        }
    }

    export class Account {
        constructor(server: FastifyInstance) {
            Channel.register(server, 'get', '/account', Account, Account.get);
            Channel.register(server, 'get', '/account/balances', Account, Account.getBalances);
            Channel.register(server, 'get', '/account/orders', Account, Account.getOrders);
            Channel.register(server, 'get', '/account/pools', Account, Account.getPools);
            Channel.register(server, 'get', '/account/pools/delegated', Account, Account.getDelegatedPools);
            Channel.register(server, 'get', '/account/delegations', Account, Account.getDelegations);
            Channel.register(server, 'get', '/account/tiers', Account, Account.getTiers);
        }
        static async getBalances(args: AccountQuery): Promise<{ asset: AssetId, unavailable: BigNumber, available: BigNumber, price: BigNumber | null }[]> {
            const account = await this.accountOf(args);
            if (!account)
                return [];

            const result = await this.balancesOf(account);
            return result;
        }
        static async getTiers(args: { pairId?: string | number } & AccountQuery): Promise<{ primary: { volume: BigNumber | null, makerFee: BigNumber | null, takerFee: BigNumber | null }, secondary: { volume: BigNumber | null, makerFee: BigNumber | null, takerFee: BigNumber | null } }> {
            const pairId = typeof args.pairId == 'string' || typeof args.pairId == 'number' ? new Uint256(args.pairId) : null;
            if (!pairId)
                throw new Error('Pair id is required');

            const dex = await Exchange.getContractByType(ContractType.DEX);
            if (!dex || !dex.account)
                throw new Error('Not a valid dex contract');

            const pair = await Exchange.getPairById(pairId);
            if (!pair || !pair.primaryAsset || !pair.secondaryAsset)
                throw new Error('Not a valid pair');

            const account = await this.accountOf(args);
            if (!account)
                throw new Error('Not a valid account');

            const fetchAccountTier = async (asset: AssetId, assetId: Uint256) => {
                const currentTier = await Blockchain.call(dex.account || '', UiUtil.toFunction(Spot.DEX.accountAssetOf), [account.address, ['$uint256', asset.toHex()]]);
                await Exchange.setSyncedAccountTierByAccountIdAndAsset(account.id, assetId, new BigNumber(currentTier.account?.volume?.toString() || ''), new BigNumber(currentTier.maker_fee?.toString() || ''), new BigNumber(currentTier.taker_fee?.toString() || ''));
            };

            let syncedPrimaryTier = await Exchange.getSyncedAccountTierByAccountIdAndAsset(account.id, pair.primaryAsset.id);
            if (!syncedPrimaryTier) {
                await fetchAccountTier(pair.primaryAsset.hash, pair.primaryAsset.id);
                syncedPrimaryTier = await Exchange.getSyncedAccountTierByAccountIdAndAsset(account.id, pair.primaryAsset.id);
            }

            let syncedSecondaryTier = await Exchange.getSyncedAccountTierByAccountIdAndAsset(account.id, pair.secondaryAsset.id);
            if (!syncedSecondaryTier) {
                await fetchAccountTier(pair.secondaryAsset.hash, pair.secondaryAsset.id);
                syncedSecondaryTier = await Exchange.getSyncedAccountTierByAccountIdAndAsset(account.id, pair.secondaryAsset.id);
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
        static async getOrders(args: { pairId?: string | number, active?: boolean } & AccountQuery & PageQuery): Promise<Order[]> {
            const filter = typeof args.active == 'boolean' ? args.active : null;
            const account = await this.accountOf(args);
            if (!account)
                return [];

            const pairId = typeof args.pairId == 'string' || typeof args.pairId == 'number' ? new Uint256(args.pairId) : null;
            if (pairId != null)
                return await Exchange.getOrdersByAccountIdAndPair(account.id, pairId, filter, Cursor.page(args.page || 0));

            return await Exchange.getOrdersByAccountId(account.id, filter, Cursor.page(args.page || 0));
        }
        static async getPools(args: { pairId?: string | number, active?: boolean } & AccountQuery & PageQuery): Promise<Pool[]> {
            const filter = typeof args.active == 'boolean' ? args.active : null;
            const account = await this.accountOf(args);
            if (!account)
                return [];

            const pairId = typeof args.pairId == 'string' || typeof args.pairId == 'number' ? new Uint256(args.pairId) : null;
            if (pairId != null)
                return await Exchange.getPoolsByAccountIdAndPair(account.id, pairId, filter, Cursor.page(args.page || 0));

            return await Exchange.getPoolsByAccountId(account.id, filter, Cursor.page(args.page || 0));
        }
        static async getDelegatedPools(args: { pairId?: string | number, active?: boolean } & AccountQuery & PageQuery): Promise<DelegatedPool[]> {
            const filter = typeof args.active == 'boolean' ? args.active : null;
            const account = await this.accountOf(args);
            if (!account)
                return [];

            const cursor = Cursor.page(args.page || 0);
            const pairId = typeof args.pairId == 'string' || typeof args.pairId == 'number' ? new Uint256(args.pairId) : null;
            const withPair = pairId != null;
            return await (withPair ? Exchange.getDelegatedPoolsByAccountIdAndPair(account.id, pairId, filter, cursor) : Exchange.getDelegatedPoolsByAccountId(account.id, filter, cursor));
        }
        static async getDelegations(args: AccountQuery): Promise<PseudoDelegatedState[]> {
            const account = await this.accountOf(args);
            if (!account)
                return [];

            return await Exchange.getDelegationOf(account.id);
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

    export class Swaps {
        private static bridgeCache: Record<string, { time: number, value: BridgeQuote }> = { };
        private static witnessCache: Record<string, { time: number, value: string[] }> = { };
        private static readonly resolverTtl = 15_000;

        constructor(server: FastifyInstance) {
            Channel.register(server, 'get', '/swaps', Swaps, Swaps.getBooking);
            Channel.register(server, 'get', '/swaps/history', Swaps, Swaps.getHistory);
            Channel.register(server, 'post', '/swaps', Swaps, Swaps.book);
            Channel.register(server, 'post', '/swaps/spend', Swaps, Swaps.spend);
            Channel.register(server, 'post', '/swaps/preview', Swaps, Swaps.preview);
            Channel.register(server, 'post', '/swaps/respend', Swaps, Swaps.respend);
            Channel.register(server, 'get', '/swaps/quote', Swaps, Swaps.getQuote);
        }
        private static sequencer(): Sequencer {
            const sequencer = Exchange.sequencer;
            if (sequencer == null)
                throw new Error('Signer is not configured; the exchange cannot execute swap requests');
            return sequencer;
        }
        private static async contract(): Promise<Contract<AmsParameters>> {
            const contract = await Exchange.getAmsContract();
            if (contract == null)
                throw new Error('AMS contract is not registered');

            return contract;
        }
        private static rawBytesToHex(value: unknown): string | null {
            if (typeof value == 'string') {
                if (value.startsWith('0x') || value.startsWith('0X'))
                    return value.toLowerCase();

                if (/^[0-9a-fA-F]+$/.test(value))
                    return '0x' + value.toLowerCase();

                if (value.length > 0 && value.length < 0x100 && [...value].every((x) => x.charCodeAt(0) < 0x100))
                    return ByteUtil.uint8ArrayToHexString(ByteUtil.byteStringToUint8Array(value)).toLowerCase();
            } else if (value instanceof Uint8Array)
                return ByteUtil.uint8ArrayToHexString(value).toLowerCase();

            return null;
        }
        private static normalizePublicKey(value: unknown): string | null {
            const hex = this.rawBytesToHex(value);
            if (hex == null || hex.length != 2 + 33 * 2)
                return null;

            const prefix = hex.substring(2, 4);
            if (prefix != '02' && prefix != '03')
                return null;

            return hex;
        }
        private static normalizeSignature(value: unknown): string | null {
            const hex = this.rawBytesToHex(value);
            if (hex == null || hex.length != 2 + Chain.size.HASHSIG * 2)
                return null;

            return hex;
        }
        private static recoverSigner(hash: Uint256, signatureHex: string): string | null {
            try {
                const data = ByteUtil.hexStringToUint8Array(signatureHex);
                if (data.length != Chain.size.HASHSIG)
                    return null;

                const signature = new Hashsig();
                signature.data = data;
                const recovered = Signing.recover(hash, signature);
                return recovered ? ByteUtil.uint8ArrayToHexString(recovered.data).toLowerCase() : null;
            } catch {
                return null;
            }
        }
        private static swapFrom(raw: unknown): BookingSwap {
            if (raw == null || typeof raw != 'object' || Array.isArray(raw))
                throw new Error('swap leg must be an object');

            const decimal = (value: unknown, name: string): BigNumber => {
                if (typeof value != 'string' && typeof value != 'number' && !BigNumber.isBigNumber(value))
                    throw new Error(name + ' must be a decimal number');

                const result = new BigNumber(value);
                if (!result.isFinite())
                    throw new Error(name + ' is not a finite number');

                return result;
            };

            const source = raw as Record<string, unknown>;
            const symbol = source.symbol == null ? '' : String(source.symbol);
            if (typeof source.asset != 'string' && typeof source.asset != 'number' && !(source.asset instanceof Uint8Array))
                throw new Error('swap leg asset must be an asset id (hex, decimal, or handle)');

            const asset = new AssetId(source.asset);
            if (!asset.isValid())
                throw new Error('swap leg asset is not a valid asset id');

            const price = decimal(source.price, 'swap leg price');
            if (!price.gt(0))
                throw new Error('swap leg price must be positive');

            const amount = decimal(source.amount, 'swap leg amount');
            if (!amount.gt(0))
                throw new Error('swap leg amount must be positive');

            const slippage = decimal(source.slippage, 'swap leg slippage');
            if (slippage.lt(0) || slippage.gte(1))
                throw new Error('swap leg slippage must be within [0; 1) range');

            return { symbol, asset, price, amount, slippage, buying: source.buying === true };
        }
        private static writeAddress(message: Stream, address: string): void {
            const decoded = Signing.decodeAddress(address);
            if (!decoded)
                throw new Error('invalid address: ' + address);

            message.writeBinaryStringOptimized(decoded.data);
        }
        private static parseSendQuota(raw: unknown): SendQuota {
            if (raw == null)
                return NoSendQuota;

            if (typeof raw != 'object' || Array.isArray(raw))
                throw new Error('Fee quote must be an object');

            const source = raw as Record<string, unknown>;
            const signature = source.signature != null ? String(source.signature) : '';
            if (signature.length < 1) {
                const fee = Common.bn(source.feeRate);
                if (fee != null && fee.gt(0))
                    throw new Error('Fee quote signature is required when a custom send fee is set');

                return NoSendQuota;
            }

            if (this.normalizeSignature(signature) == null)
                throw new Error('Fee quote signature must be a 65-byte recoverable signature');

            const blockNumber = Common.bn(source.blockNumber);
            if (blockNumber == null || blockNumber.lt(0))
                throw new Error('Fee quote block number is invalid');

            const feeRate = Common.bn(source.feeRate);
            if (feeRate == null || !feeRate.gt(0))
                throw new Error('Fee quote fee must be a positive decimal');
            return { signature: signature, blockNumber: blockNumber.toFixed(), feeRate: feeRate.toFixed() };
        }
        private static writeQuota(stream: Stream, quota: SendQuota, asArguments: boolean): void {
            const signature = quota.signature.length > 0 ? ByteUtil.hexStringToUint8Array(quota.signature) : new Uint8Array(65);
            if (asArguments)
                stream.writeBinaryStringOptimized(signature);
            else
                stream.writeBinaryString(signature);
            stream.writeInteger(new Uint256(quota.blockNumber));
            stream.writeDecimal(new BigNumber(quota.feeRate), !asArguments);
        }
        private static encodeQuotaArguments(quota: SendQuota): string {
            const stream = new Stream();
            this.writeQuota(stream, quota, true);
            return stream.encode();
        }
        private static writeQuotaMessage(message: Stream, quota: SendQuota): void {
            this.writeQuota(message, quota, false);
        }
        private static encodeSpendMessage(params: SpendMessageParams): Uint8Array {
            const message = new Stream();
            message.writeInteger(params.depositHash);
            message.writeInteger(params.bridgeHash);
            message.writeInteger(params.inputAsset.toUint256());
            message.writeDecimal(params.inputAmount, true);
            this.writeAddress(message, params.contractAddress);
            this.writeAddress(message, params.sequencerAddress);
            message.writeString(params.routingAddress);
            this.writeQuotaMessage(message, params.quota);
            message.writeInteger(params.tokenPath.length + params.feePath.length);
            for (let order of params.tokenPath.concat(params.feePath)) {
                message.writeString(order.symbol);
                message.writeInteger(order.asset.toUint256());
                message.writeDecimal(order.price);
                message.writeDecimal(order.amount);
                message.writeDecimal(order.slippage);
                message.writeBoolean(order.buying);
            }

            return message.data;
        }
        private static encodeRespendMessage(params: RespendMessageParams): Uint8Array {
            const message = new Stream();
            message.writeInteger(params.depositHash);
            message.writeInteger(params.broadcastHash);
            message.writeInteger(params.bridgeHash);
            message.writeInteger(params.asset.toUint256());
            message.writeDecimal(params.amount, true);
            this.writeAddress(message, params.contractAddress);
            this.writeAddress(message, params.sequencerAddress);
            message.writeString(params.routingAddress);
            this.writeQuotaMessage(message, params.quota);
            return message.data;
        }
        private static async constCall(contractAccount: string, method: string, args: unknown[]): Promise<unknown> {
            const receipt = await RPC.callTransaction(new AssetId(), Exchange.sequencer?.address ?? '', contractAccount, method, args);
            if (receipt == null)
                throw new Error(`constant call ${method} returned no receipt`);

            return receipt.result;
        }
        private static buildCall(options: SwapExecution, nonce: Uint256, gasPrice: BigNumber, gasLimit: Uint256): Stream {
            const transaction = {
                signature: new Hashsig(),
                asset: new AssetId(),
                nonce: nonce,
                gasPrice: gasPrice,
                gasLimit: gasLimit,
                callable: Signing.decodeAddress(options.contractAccount),
                pays: [],
                function: (options.bridgeSend === true ? '^' : '') + UiUtil.toFunction(options.contractFunction),
                args: options.args
            };

            let stream = new Stream();
            SchemaUtil.store(stream, transaction, Messages.asSigningSchema(new Transactions.Call()));

            const sequencer = this.sequencer();
            const signature = Signing.sign(stream.hash(), sequencer.secretKey);
            if (!signature)
                throw new Error('failed to sign a transaction');

            stream = new Stream();
            SchemaUtil.store(stream, { ...transaction, signature: signature }, new Transactions.Call());
            return stream;
        }
        private static async sequencerNonce(): Promise<Uint256> {
            return new Uint256((await RPC.getNextAccountNonce(this.sequencer().address))?.toString());
        }
        private static async sequencerGasPrice(): Promise<BigNumber> {
            return new BigNumber((await RPC.getGasPrice(new AssetId(), 0.95))?.price.toString() || 0);
        }
        private static nextBlockSignal(): Promise<void> {
            return new Promise((resolve) => {
                const off = Blockchain.addBlockListener(() => {
                    off();
                    resolve();
                });
                setTimeout(() => {
                    off();
                    resolve();
                }, 2_000);
            });
        }
        private static async execute(options: SwapExecution): Promise<string | null> {
            const sequencer = this.sequencer();
            const nonce = await this.sequencerNonce();
            const gasPrice = await this.sequencerGasPrice();
            if (sequencer.maxGasPrice != null && gasPrice.gt(sequencer.maxGasPrice))
                throw new Error(`gas price ${gasPrice.toString()} exceeds policy cap ${sequencer.maxGasPrice}`);

            const build = (gasLimit: Uint256): Stream => this.buildCall(options, nonce, gasPrice, gasLimit);
            const simulation = build(new Uint256(1_000_000));
            try {
                const receipt = await RPC.simulateTransaction(simulation.encode());
                const gasLimit = new Uint256(receipt?.relative_gas_use?.toString() || 0);
                if (!receipt?.successful)
                    throw new Error(`${options.describe} rejected by simulation: ${JSON.stringify(receipt?.message ?? receipt?.error ?? 'unknown simulation error')}`);

                if (!gasLimit.gt(0))
                    throw new Error('failed to simulate transaction');

                const transactionHash = await RPC.submitTransaction(build(gasLimit).encode());
                if (!transactionHash)
                    throw new Error('failed to submit transaction');

                const deadline = Date.now() + sequencer.confirmTimeout * 1_000;
                while (true) {
                    await this.nextBlockSignal();
                    let confirmation: unknown = null;
                    try {
                        confirmation = await RPC.getTransactionByHash(transactionHash);
                    } catch {
                        confirmation = null;
                    }

                    if (confirmation)
                        break;

                    if (Date.now() >= deadline)
                        throw new Error(`transaction ${transactionHash} not confirmed within ${sequencer.confirmTimeout}s`);
                }

                Log.info(`${options.describe} finalized (tx: ${transactionHash})`);
                return transactionHash;
            } catch (exception) {
                Log.info('TX message:', simulation.encode());
                throw exception;
            }
        }
        private static async spendContext(args: Record<string, any>): Promise<{ booking: Booking, contract: Contract<AmsParameters>, signature: string, callArgs: unknown[] }> {
            const sequencer = this.sequencer();
            const depositHash = Common.u256(args.depositHash);
            if (depositHash == null)
                throw new Error('Deposit hash is required');

            const booking = await Exchange.getBookingByDepositHash(depositHash);
            if (booking == null)
                throw new Error('Deposit does not match a booking (multi-asset payment or unknown booking)');

            if (booking.depositBlock == null)
                throw new Error('Deposit is not mined yet');

            const contract = await this.contract();

            const bridgeHash = Common.u256(args.bridgeHash);
            if (bridgeHash == null || bridgeHash.compareTo(new Uint256()) <= 0)
                throw new Error('Bridge hash must be set');

            if (typeof args.routingAddress != 'string' || args.routingAddress.length < 1)
                throw new Error('Routing address is required');

            const parsePath = (raw: unknown, name: string): BookingSwap[] => {
                if (raw == null)
                    return [];

                if (!Array.isArray(raw))
                    throw new Error(name + ' must be an array');

                return raw.map((leg) => this.swapFrom(leg));
            };

            const tokenPath = parsePath(args.tokenPath, 'Token path');
            const feePath = parsePath(args.feePath, 'Fee path');
            for (let i = 0; i < tokenPath.length; i++) {
                const previous = i == 0 ? booking.asset : tokenPath[i - 1].asset;
                if (tokenPath[i].asset.equals(previous))
                    throw new Error(`token path leg ${i} swaps an asset into itself`);
            }

            const finalAsset = tokenPath.length > 0 ? tokenPath[tokenPath.length - 1].asset : booking.asset;
            const baseAsset = AssetId.fromHandle(finalAsset.chain || Chain.policy.TOKEN_NAME);
            if (finalAsset.equals(baseAsset)) {
                if (feePath.length > 0)
                    throw new Error('fee path must be empty when the output asset is already the network base asset');
            } else if (feePath.length == 0)
                throw new Error('fee path is required when the output asset differs from the network base asset');

            if (feePath.length > 0 && !feePath[feePath.length - 1].asset.equals(baseAsset))
                throw new Error(`fee path must end at the network base asset (${baseAsset.id})`);

            const signature = this.normalizeSignature(args.signature);
            if (signature == null)
                throw new Error('Signature must be a 65-byte recoverable signature');

            const quota = this.parseSendQuota(args.feeQuote);
            const message = this.encodeSpendMessage({
                depositHash: depositHash,
                bridgeHash: bridgeHash,
                inputAsset: booking.asset,
                inputAmount: booking.amount,
                contractAddress: contract.account || '',
                sequencerAddress: sequencer.address,
                routingAddress: args.routingAddress,
                tokenPath: tokenPath,
                feePath: feePath,
                quota: quota
            });
            const signer = this.recoverSigner(new Uint256(Hashing.hash256(message)), signature);
            const expected = this.normalizePublicKey(booking.publicKey);
            if (signer == null || expected == null || signer != expected)
                throw new Error('Spend signature does not match the booking public key');

            const top = await Exchange.getLatestBlock();
            if (top == null)
                throw new Error('Blockchain is not synced yet');

            if (top.blockNumber - booking.depositBlock > contract.parameters.bookValidityBlocks)
                throw new Error('Deposit transaction has timed out');

            if (top.blockNumber > booking.expiresBlock)
                throw new Error('Booking has expired');

            if (await this.constCall(contract.account || '', Spot.AMS.bookingAlive, [booking.asset.toUint256(), booking.amount]) !== true)
                throw new Error('Booking is not alive on-chain (expired or spent)');

            const encodePath = (path: BookingSwap[]): string => {
                const stream = new Stream();
                stream.writeInteger(path.length);
                for (let order of path) {
                    stream.writeString(order.symbol);
                    stream.writeInteger(order.asset.toUint256());
                    stream.writeDecimal(order.price);
                    stream.writeDecimal(order.amount);
                    stream.writeDecimal(order.slippage);
                    stream.writeBoolean(order.buying);
                }

                return stream.encode();
            };

            return {
                booking: booking,
                contract: contract,
                signature: signature,
                callArgs: [depositHash, bridgeHash, args.routingAddress, encodePath(tokenPath), encodePath(feePath), this.encodeQuotaArguments(quota), ByteUtil.hexStringToUint8Array(signature)]
            };
        }
        static async getBooking(args: Record<string, any>): Promise<Booking> {
            const contract = await this.contract();
            if (args.id != null) {
                const id = Common.u256(args.id);
                if (id == null)
                    throw new Error('Booking id is not a valid id');

                const booking = await Exchange.getBookingById(id);
                if (booking == null)
                    throw new Error('Booking not found');

                return booking;
            }

            if (typeof args.asset != 'string' && typeof args.asset != 'number')
                throw new Error('Asset is required');

            const asset = new AssetId(args.asset);
            if (!asset.isValid())
                throw new Error('Asset is not a valid asset id');

            const assetId = await Exchange.getAssetIdByHash(asset, 'trusted');
            if (assetId == null)
                throw new Error('Asset not found: ' + asset.id);

            const amount = Common.bn(args.amount);
            if (amount == null || !amount.gt(0))
                throw new Error('Amount must be a positive decimal');

            const booking = await Exchange.getBookingByHandle(assetId, amount.decimalPlaces(contract.parameters.depositPrecision ?? 6, BigNumber.ROUND_DOWN), null);
            if (booking == null)
                throw new Error('Booking not found');

            return booking;
        }
        static async getHistory(args: PageQuery & { publicKey?: unknown, publicKeys?: unknown, status?: unknown }): Promise<Booking[]> {
            const raw = args.publicKeys == null ? [] : (Array.isArray(args.publicKeys) ? args.publicKeys : [args.publicKeys]);
            if (args.publicKey != null)
                raw.push(args.publicKey);

            const keys: string[] = [];
            for (const value of raw) {
                const key = this.normalizePublicKey(value);
                if (key == null)
                    throw new Error('Public key filter must be a valid compressed secp256k1 point (epoint264, 0x hex)');
                if (!keys.includes(key))
                    keys.push(key);
            }

            const rawStatuses = args.status == null ? [] : (Array.isArray(args.status) ? args.status : [args.status]);
            const statuses: string[] = [];
            for (const value of rawStatuses) {
                const status = typeof value == 'string' ? value : null;
                if (status == null || !Object.values(BookingStatus).includes(status as BookingStatus))
                    throw new Error('Status filter must be a known booking status');
                if (!statuses.includes(status))
                    statuses.push(status);
            }

            return await Exchange.getBookings(Cursor.page(args.page || 0), keys.length > 0 ? keys : null, statuses.length > 0 ? statuses : null);
        }
        private static recordOf(value: unknown): Record<string, unknown> | null {
            return typeof value == 'object' && value != null && !Array.isArray(value) ? value as Record<string, unknown> : null;
        }
        private static arrayOf(value: unknown): unknown[] {
            return Array.isArray(value) ? value : [];
        }
        private static textOf(value: unknown): string | null {
            return typeof value == 'string' ? value : (typeof value == 'number' ? String(value) : null);
        }
        private static stringsOf(value: unknown): string[] {
            return Swaps.arrayOf(value).filter((entry): entry is string => typeof entry == 'string' && entry.length > 0);
        }
        private static async witnessAddresses(chain: string): Promise<string[]> {
            const cached = Swaps.witnessCache[chain];
            if (cached != null && Date.now() - cached.time < Swaps.resolverTtl)
                return cached.value;

            const ams = await Exchange.getAmsContract();
            const addresses: string[] = [];
            for (const raw of Swaps.arrayOf(await RPC.getWitnessAccounts(ams?.account ?? '', 0, 64))) {
                const row = Swaps.recordOf(raw);
                if (Swaps.textOf(row?.['purpose']) != 'bridge')
                    continue;
                if (chain.length > 0 && Swaps.textOf(Swaps.recordOf(row?.['asset'])?.['chain']) != chain)
                    continue;

                for (const value of Swaps.stringsOf(row?.['addresses'])) {
                    if (!addresses.includes(value))
                        addresses.push(value);
                }
            }

            Swaps.witnessCache[chain] = { time: Date.now(), value: addresses };
            return addresses;
        }
        static async getQuote(args: { assetOut?: unknown, amount?: unknown, assetIn?: unknown, slippage?: unknown }): Promise<BridgeQuote> {
            if (typeof args.assetOut != 'string' && typeof args.assetOut != 'number')
                throw new Error('assetOut is required');

            const asset = new AssetId(args.assetOut);
            if (!asset.isValid())
                throw new Error('assetOut is not a valid asset id');

            const amount = args.amount != null ? Common.bn(args.amount) ?? null : null;
            if (args.amount != null && !(amount != null && amount.gt(0)))
                throw new Error('Amount must be a positive decimal');

            const assetIn = typeof args.assetIn == 'string' && args.assetIn.length > 0 ? args.assetIn : null;
            if (assetIn != null && (amount == null || args.slippage == null))
                throw new Error('assetIn requires both amount and slippage to route');

            const slippage = args.slippage != null ? Common.bn(args.slippage) ?? null : null;
            if (args.slippage != null && !(slippage != null && slippage.gte(0) && slippage.lte(1)))
                throw new Error('Slippage must be between [0; 1]');

            const key = asset.id + ':' + (amount != null ? amount.toFixed() : '*') + ':' + (assetIn != null && amount != null && slippage != null ? assetIn + ':' + slippage.toFixed() : '-');
            const cached = Swaps.bridgeCache[key];
            if (cached != null && Date.now() - cached.time < Swaps.resolverTtl)
                return cached.value;

            const chain = asset.chain ?? '';
            const [witnesses, descriptors] = await Promise.all([Swaps.witnessAddresses(chain), Blockchain.getBlockchains()]);
            const routingPolicy = descriptors.find((descriptor) => descriptor.chain == chain)?.routingPolicy ?? null;

            let path: RouterPath[] | null = null;
            if (amount != null && amount.gt(0) && assetIn != null && slippage != null) {
                const assetIdIn = await Exchange.getAssetIdByHash(new AssetId(assetIn), 'untrusted');
                const assetIdOut = await Exchange.getAssetIdByHash(asset, 'untrusted');
                path = assetIdIn != null && assetIdOut != null ? await Exchange.getRoutingPathsByAssetIds(assetIdIn, assetIdOut, amount, slippage, 6) : [];
            }

            const bestPath = path != null && path.length > 0 ? path[0] : null;
            const bestLeg = bestPath != null && bestPath.length > 0 ? bestPath[bestPath.length - 1] : null;
            const masters: Record<string, string | null> = { };
            const candidates: { hash: string, chain: string, feeRate: BigNumber, supply: BigNumber | null, master: string | null }[] = [];
            for (const raw of Swaps.arrayOf(await RPC.getBestBridgeInstancesByBalance(asset, 0, 8))) {
                const row = Swaps.recordOf(raw);
                const instance = Swaps.recordOf(row?.['instance']) ?? row;
                if (instance == null)
                    continue;

                const hash = Swaps.textOf(instance['bridge_hash'] ?? instance['bridgeHash']);
                if (hash == null)
                    continue;

                const instanceChain = Swaps.textOf(Swaps.recordOf(instance['asset'])?.['chain'] ?? instance['chain']) ?? chain;
                const masterAddress = Swaps.textOf(instance['master']);
                let master: string | null = null;
                if (masterAddress != null) {
                    if (masters[masterAddress] === undefined) {
                        masters[masterAddress] = null;
                        try {
                            for (const witnessRaw of Swaps.arrayOf(await RPC.getWitnessAccountsByPurpose(masterAddress, 'bridge', 0, 32))) {
                                const witness = Swaps.recordOf(witnessRaw);
                                const witnessHash = Swaps.textOf(witness?.['bridge_hash']);
                                const addresses = Swaps.stringsOf(witness?.['addresses']);
                                if (Swaps.textOf(Swaps.recordOf(witness?.['asset'])?.['chain']) == instanceChain && (witnessHash == null || witnessHash === hash) && addresses.length > 0) {
                                    masters[masterAddress] = addresses[0];
                                    break;
                                }
                            }
                        } catch (exception) {
                            Log.error('relay', 'failed resolving bridge master (master:', masterAddress, '):', exception);
                        }
                    }
                    master = masters[masterAddress];
                }

                const balance = Swaps.recordOf(row?.['balances'])?.[asset.id];
                candidates.push({
                    hash: hash,
                    chain: instanceChain,
                    feeRate: Common.bn(instance['fee_rate'] ?? instance['feeRate']) ?? new BigNumber(0),
                    supply: Common.bn(Swaps.recordOf(balance)?.['supply'] ?? balance) ?? null,
                    master: master
                });
            }
            
            const requirement = bestLeg != null ? (bestLeg.output.max.gt(0) ? bestLeg.output.max : bestLeg.output.min) : amount;
            const funded = requirement != null && requirement.gt(0) ? candidates.filter((candidate) => candidate.supply != null && candidate.supply.gte(requirement)) : candidates;
            const picked = funded.length > 0 ? funded[0] : candidates[0];
            if (!picked)
                throw new Error('Failed to find a route for this swap');

            let sendQuota: SendQuota = NoSendQuota;
            try {
                const feeQuotes = await RPC.getFeeQuotes(asset);
                const quoteIndex = Common.bn(feeQuotes?.index)?.toNumber() ?? -1;
                const bestQuote = feeQuotes != null && quoteIndex >= 0 ? feeQuotes.quotes[quoteIndex] : null;
                if (bestQuote != null && typeof bestQuote.signature == 'string' && bestQuote.signature.length > 0 && bestQuote.fee != null && bestQuote.fee.gt(0) && bestQuote.fee.lte(picked.feeRate))
                    sendQuota = { signature: bestQuote.signature, blockNumber: bestQuote.expiry_block_number?.toFixed() ?? '0', feeRate: bestQuote.fee.toFixed() };
            } catch (exception) {
                Log.error('relay', `failed resolving send fee quote (asset: ${asset.id}):`, exception);
            }

            const quote: BridgeQuote = {
                hash: picked.hash,
                chain: picked.chain,
                feeRate: picked.feeRate.toFixed(),
                supply: picked.supply != null ? picked.supply.toFixed() : null,
                master: picked.master,
                witnesses: witnesses,
                routingPolicy: routingPolicy,
                path: path,
                sendQuota: sendQuota,
                sendFee: sendQuota.signature.length > 0 ? sendQuota.feeRate : picked.feeRate.toFixed()
            };
            Swaps.bridgeCache[key] = { time: Date.now(), value: quote };
            return quote;
        }
        static async book(args: { contract?: string, asset?: string | number, amount?: string | number, publicKey?: string }, _response?: FastifyReply, meta?: CallMeta): Promise<SwapReference> {
            const limit: BookingAcquisition = BookingRateLimit.acquireBooking(meta != null ? meta.ip : null);
            if (!limit.ok) {
                const exception = new Error(limit.scope == 'instance'
                    ? `Swap booking capacity reached, retry in ${limit.retryAfter} second${limit.retryAfter == 1 ? '' : 's'}`
                    : `Too many swap bookings from your address, retry in ${limit.retryAfter} second${limit.retryAfter == 1 ? '' : 's'}`) as Error & { retryAfter?: number };
                exception.retryAfter = limit.retryAfter;
                throw exception;
            }
            const contract = await this.contract();
            this.sequencer();
            if (typeof args.asset != 'string' && typeof args.asset != 'number')
                throw new Error('Asset is required');

            let asset: AssetId;
            try {
                asset = new AssetId(args.asset);
            } catch {
                throw new Error('Asset is not a valid asset id');
            }
            if (!asset.isValid())
                throw new Error('Asset is not a valid asset id');

            const assetId = await Exchange.getAssetIdByHash(asset, 'trusted');
            if (assetId == null)
                throw new Error('Asset not found: ' + asset.id);

            let amount = Common.bn(args.amount);
            if (amount == null || !amount.gt(0))
                throw new Error('Amount must be a positive decimal');
            
            const precision = contract.parameters.depositPrecision ?? 6;
            amount = amount.decimalPlaces(precision, BigNumber.ROUND_DOWN);
            if (amount == null || !amount.gt(0))
                throw new Error(`Amount is too small to book: it truncates to zero at ${precision} decimals`);

            const publicKey = this.normalizePublicKey(args.publicKey);
            if (publicKey == null)
                throw new Error('Public key must be a valid compressed secp256k1 point (epoint264, 0x hex)');

            const openStatuses = Object.values(BookingStatus).filter((v) => v != BookingStatus.Finalized && v != BookingStatus.Expired);
            const tick = new BigNumber(10).pow(-precision);
            let reserved = false;
            for (let attempt = 0; attempt < 8 && !reserved; attempt++) {
                const taken = await Exchange.getBookingByHandle(assetId, amount, openStatuses);
                if (taken == null) {
                    reserved = true;
                    break;
                }
                amount = amount.minus(tick);
                if (!amount.gt(0))
                    break;
            }
            if (!reserved)
                throw new Error('Could not reserve a unique amount for this asset');

            const transaction = await Exchange.withExclusiveLock(() => this.execute({
                contractAccount: contract.account || '',
                contractFunction: Spot.AMS.bookDeposit,
                args: [asset.toUint256(), amount, ByteUtil.hexStringToUint8Array(publicKey)],
                describe: `book ${asset.id} ${amount.toString()} @ ${contract.account}`
            }));
            BookingRateLimit.commitBooking(limit);

            return { transaction: transaction, amount: amount.toString() };
        }
        static async spend(args: { depositHash?: string, bridgeHash?: string, routingAddress?: string, tokenPath?: unknown, feePath?: unknown, feeQuote?: unknown, signature?: string }): Promise<SwapReference> {
            const context = await this.spendContext(args);
            const depositHash = Common.u256(args.depositHash)!, contract = context.contract;
            const routing = String(args.routingAddress);
            const transaction = await Exchange.withExclusiveLock(async () => {
                const liveBooking = await Exchange.getBookingById(context.booking.id);
                if (liveBooking == null || liveBooking.status != BookingStatus.AwaitingExecution)
                    throw new Error('Booking is no longer awaiting execution');

                if (liveBooking.depositBlock == null)
                    throw new Error('Deposit is not mined yet');

                const top = await Exchange.getLatestBlock();
                if (top != null && contract.parameters.bookValidityBlocks > 0 && top.blockNumber - liveBooking.depositBlock > contract.parameters.bookValidityBlocks)
                    throw new Error('Deposit transaction has timed out');

                if (top != null && top.blockNumber > liveBooking.expiresBlock)
                    throw new Error('Booking has expired');

                if (await this.constCall(contract.account || '', Spot.AMS.bookingAlive, [liveBooking.asset.toUint256(), liveBooking.amount]) !== true)
                    throw new Error('Booking is not alive on-chain (expired or spent)');

                return await this.execute({
                    contractAccount: contract.account || '',
                    contractFunction: Spot.AMS.spendDeposit,
                    args: context.callArgs,
                    describe: `spend ${depositHash.toHex()} @ ${contract.account}`,
                    bridgeSend: true
                });
            });

            await Exchange.setBookingRouting(context.booking.id, routing);
            return { transaction: transaction };
        }
        static async preview(args: { depositHash?: string, bridgeHash?: string, routingAddress?: string, tokenPath?: unknown, feePath?: unknown, feeQuote?: unknown, signature?: string }): Promise<{ successful: boolean, gas: string | null, outAsset: string | null, outAmount: string | null, error: string | null }> {
            const fail = (message: string) => ({ successful: false, gas: null, outAsset: null, outAmount: null, error: message });
            try {
                const context = await this.spendContext(args);
                const nonce = await this.sequencerNonce();
                const gasPrice = await this.sequencerGasPrice();
                const stream = this.buildCall({
                    contractAccount: context.contract.account || '',
                    contractFunction: Spot.AMS.spendDeposit,
                    args: context.callArgs,
                    describe: `preview spend ${args.depositHash} @ ${context.contract.account}`,
                    bridgeSend: true
                }, nonce, gasPrice, new Uint256(1_000_000));

                const receipt = await RPC.simulateTransaction(stream.encode());
                if (receipt == null)
                    return fail('Simulation returned no receipt');

                if (!receipt.successful)
                    return fail(`Rejected by simulation: ${String(receipt.message ?? receipt.error ?? 'unknown simulation error')}`);

                const events: any[] = Array.isArray(receipt.events) ? receipt.events : [];
                const swapRef = events.find((event: any) => event.event == Spot.AMS.Events.SwapRef && Array.isArray(event.args) && event.args.length >= 4);
                const outAmount = swapRef != null ? Common.bn(swapRef.args[3]) : null;
                return {
                    successful: true,
                    gas: receipt.relative_gas_use?.toString() ?? null,
                    outAsset: swapRef != null ? String(swapRef.args[2]) : null,
                    outAmount: outAmount != null ? outAmount.toString() : null,
                    error: null
                };
            } catch (exception: any) {
                return fail(String(exception?.message ?? exception));
            }
        }
        static async respend(args: { depositHash?: string, broadcastHash?: string, bridgeHash?: string, routingAddress?: string, feeQuote?: unknown, signature?: string }): Promise<SwapReference> {
            const sequencer = this.sequencer();
            const depositHash = Common.u256(args.depositHash);
            if (depositHash == null)
                throw new Error('Deposit hash is required');

            const booking = await Exchange.getBookingByDepositHash(depositHash);
            if (booking == null)
                throw new Error('Deposit does not match a booking');

            if (booking.status != BookingStatus.FailingWithdrawal)
                throw new Error(`Booking status (${booking.status}) does not accept a respend request`);

            const contract = await this.contract();

            const broadcastHash = Common.u256(args.broadcastHash);
            if (broadcastHash == null)
                throw new Error('Broadcast hash is required');

            if (booking.broadcastHash != null && booking.broadcastHash != broadcastHash.toHex())
                throw new Error('Broadcast hash does not match the recorded failing withdrawal attempt');

            const bridgeHash = Common.u256(args.bridgeHash);
            if (bridgeHash == null || bridgeHash.compareTo(new Uint256()) <= 0)
                throw new Error('Bridge hash must be set');

            if (typeof args.routingAddress != 'string' || args.routingAddress.length < 1)
                throw new Error('Routing address is required');

            if (booking.outAsset == null || booking.outAmount == null)
                throw new Error('Booking has no recorded swap output to respend');

            const signature = this.normalizeSignature(args.signature);
            if (signature == null)
                throw new Error('Signature must be a 65-byte recoverable signature');
            
            const quota = this.parseSendQuota(args.feeQuote);
            const message = this.encodeRespendMessage({
                depositHash: depositHash,
                broadcastHash: broadcastHash,
                bridgeHash: bridgeHash,
                asset: booking.outAsset,
                amount: booking.outAmount.decimalPlaces(contract.parameters.depositPrecision ?? 6, BigNumber.ROUND_DOWN),
                contractAddress: contract.account || '',
                sequencerAddress: sequencer.address,
                routingAddress: args.routingAddress,
                quota: quota
            });
            const signer = this.recoverSigner(new Uint256(Hashing.hash256(message)), signature);
            const expected = this.normalizePublicKey(booking.publicKey);
            if (signer == null || expected == null || signer != expected)
                throw new Error('Respend signature does not match the booking public key');

            if (booking.bridgeHash != null && booking.bridgeHash != bridgeHash.toHex())
                Log.info(`ams respend: bridge hash changed (${booking.bridgeHash} -> ${bridgeHash.toHex()}); fee-rise rule enforced by contract simulation at submission time`);

            const routing = args.routingAddress;
            const transaction = await Exchange.withExclusiveLock(async () => {
                const liveBooking = await Exchange.getBookingById(booking.id);
                if (liveBooking == null || liveBooking.status != BookingStatus.FailingWithdrawal)
                    throw new Error('Booking is no longer a failing withdrawal');

                if (liveBooking.broadcastHash != null && liveBooking.broadcastHash != broadcastHash.toHex())
                    throw new Error('Broadcast hash does not match the recorded failing withdrawal attempt');

                const top = await Exchange.getLatestBlock();
                if (top != null && top.blockNumber > liveBooking.expiresBlock)
                    throw new Error('Booking has expired');

                if (await this.constCall(contract.account || '', Spot.AMS.depositSpent, [depositHash]) !== true)
                    throw new Error('Deposit is not spent on-chain; nothing to respend');

                return await this.execute({
                    contractAccount: contract.account || '',
                    contractFunction: Spot.AMS.respendDeposit,
                    args: [
                        depositHash,
                        broadcastHash,
                        bridgeHash,
                        routing,
                        this.encodeQuotaArguments(quota),
                        ByteUtil.hexStringToUint8Array(signature)
                    ],
                    describe: `respend ${depositHash.toHex()} @ ${contract.account}`,
                    bridgeSend: true
                });
            });
            await Exchange.setBookingRouting(booking.id, routing);

            return { transaction: transaction };
        }
    }

    export class Channel {
        static routes: Record<string, { context: any, callback: (args?: any, response?: FastifyReply, meta?: CallMeta) => Promise<any> }> = { };

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

                        const result = await target.callback.apply(target.context, [params, undefined, { ip: channel.ip }]);
                        return Result.data(result, args.id);
                    }
                }

                throw new Error('Illegal operation');
            } catch (exception: any) {
                return Result.error(exception, args.id);
            }
        }
        static register(server: FastifyInstance, method: 'get' | 'post', path: string, context: any, callback: (args?: any, response?: FastifyReply, meta?: CallMeta) => Promise<any>, asIs?: boolean): (args?: any, response?: FastifyReply, meta?: CallMeta) => Promise<any> {
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