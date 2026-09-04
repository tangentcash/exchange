import pq from 'postgres';
import { AssetId } from 'tangentsdk';
import { Exchange, Notification } from './exchange'
import { QuoteResult, Quotes, symbolOf } from './market';
import { OrderSide, Trade } from '../types';
import { Log } from '../logging';
import BigNumber from 'bignumber.js';

export class Jobs {
    static assetPrices: {
        blacklist: Set<string>,
        timeout: NodeJS.Timeout | null,
    } = {
        blacklist: new Set<string>(),
        timeout: null
    };
    static assetCleanup: NodeJS.Timeout | null = null;
    static pairCleanup: NodeJS.Timeout | null = null;
    static marketData: NodeJS.Timeout | null = null;
    
    static async runAssetPrices(interval: number, runDeferred?: boolean): Promise<void> {
        return new Promise<void>((resolve, reject) => {
            if (this.assetPrices.timeout != null)
                return resolve();

            this.assetPrices.timeout = setTimeout(async () => {
                try {
                    const baseSymbol = Quotes.globalBase();
                    if (!baseSymbol)
                        throw new Error('global base must be set');

                    const base = AssetId.fromHandle(baseSymbol);
                    const assets = await Exchange.getAssetHandles();
                    const cache: Record<string, Omit<Trade, 'id' | 'pairId'>> = { };
                    const trades: { asset: AssetId, trade: Omit<Trade, 'id' | 'pairId'> }[] = [];
                    const fits = { realtime: 0, fallback: 0, cache: 0 };
                    const native = new AssetId().id;
                    for (let i = 0; i < assets.length; i++) {
                        const asset = assets[i];
                        if (asset.id == native || this.assetPrices.blacklist.has(asset.id))
                            continue;
                        
                        const symbol = symbolOf(asset);
                        const parent = cache[symbol];
                        try {
                            const price: QuoteResult = parent ? { value: parent.price, source: 'cache' } : await Quotes.crossPriceOf(asset, base);
                            const delta = 2 * Math.random() - 1;
                            let trade: Omit<Trade, 'id' | 'pairId'> = parent ? parent : {
                                side: delta > 0 ? OrderSide.Buy : OrderSide.Sell,
                                price: price.value.multipliedBy(1 + delta * 0.0001),
                                quantity: new BigNumber(0),
                                time: new Date()
                            };
                            ++fits[price.source];
                            if (!parent)
                                cache[symbol] = trade;

                            trades.push({ asset: asset, trade: trade });
                        } catch (exception: any) {
                            Log.info(`job ${symbol} market sync failed:`, exception);
                            if (Quotes.isWhitelistingError(exception))
                                this.assetPrices.blacklist.add(asset.id);
                        }
                    }

                    Log.info(`job market sync: OK complete (trades: ${trades.length}/${assets.length}, realtime: ${fits.realtime}, fallback: ${fits.fallback}, cache: ${fits.cache})`);
                    await Exchange.isolate(async (connection: pq.TransactionSql) => {
                        for (let i = 0; i < trades.length; i++) {
                            const trade = trades[i];
                            try {
                                const pairId = await Exchange.getPairByAssetHashes(trade.asset, null, null, true, true, connection);
                                if (!pairId)
                                    throw new Error('invalid pair id');

                                const result = await Exchange.setTrade({ pairId: pairId, ...trade.trade }, connection);
                                if (!result)
                                    throw new Error('invalid trade');

                                await Exchange.notify(Notification.TradeUpdate, {
                                    query: { },
                                    args: {
                                        primaryAsset: trade.asset,
                                        secondaryAsset: null,
                                        secondaryBase: base.handle,
                                        account: null,
                                        side: result.side,
                                        price: result.price,
                                        quantity: result.quantity,
                                    }
                                }, connection);
                            } catch (exception: any) {
                                Log.info(`job ${symbolOf(trade.asset)} market sync failed:`, exception);
                                if (Quotes.isWhitelistingError(exception))
                                    this.assetPrices.blacklist.add(trade.asset.id);
                            }
                        }
                    });
                    this.assetPrices.timeout = null;
                    resolve();
                } catch (exception) {
                    this.assetPrices.timeout = null;
                    reject(exception);
                }
                this.runAssetPrices(interval, true);
            }, runDeferred ? interval : 0);
        });
    }
    static async runAssetCleanup(interval: number, runDeferred?: boolean): Promise<void> {
        return new Promise<void>((resolve, reject) => {
            this.assetCleanup = setTimeout(async () => {
                try {
                    try {
                        const result = await Exchange.eraseGarbageAssets();
                        Log.info(`job asset cleanup: ${result} assets erased`);
                    } catch (exception) {
                        Log.error(`job asset cleanup error:`, exception);
                    }

                    Exchange
                    this.assetCleanup = null;
                    resolve();
                } catch (exception) {
                    this.assetCleanup = null;
                    reject(exception);
                }
                Exchange.clearCache();
                this.runAssetCleanup(interval, true);
            }, runDeferred ? interval : 0);
        });
    }
    static async runPairCleanup(interval: number, runDeferred?: boolean): Promise<void> {
        return new Promise<void>((resolve, reject) => {
            this.pairCleanup = setTimeout(async () => {
                try {
                    try {
                        const result = await Exchange.eraseGarbagePairs();
                        Log.info(`job pair cleanup: ${result} pairs erased`);
                    } catch (exception) {
                        Log.error(`job pair cleanup error:`, exception);
                    }

                    this.pairCleanup = null;
                    resolve();
                } catch (exception) {
                    this.pairCleanup = null;
                    reject(exception);
                }
                Exchange.clearCache();
                this.runPairCleanup(interval, true);
            }, runDeferred ? interval : 0);
        });
    }
    static async runMarketData(interval: number, runDeferred?: boolean): Promise<void> {
        return new Promise<void>((resolve, reject) => {
            this.marketData = setTimeout(async () => {
                try {
                    try {
                        await Exchange.precomputeMarketData();
                        Log.info(`job market data: OK`);
                    } catch (exception) {
                        Log.error(`job market data error:`, exception);
                    }

                    this.marketData = null;
                    resolve();
                } catch (exception) {
                    this.marketData = null;
                    reject(exception);
                }
                this.runMarketData(interval, true);
            }, runDeferred ? interval : 0);
        });
    }
}