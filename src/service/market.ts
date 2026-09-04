import { AssetId, Whitelist } from 'tangentsdk';
import { Log } from './../logging';
import BigNumber from 'bignumber.js';

const TTL = {
    realtime: 30_000,
    fallback: 240_000
};

export function symbolOf(asset: AssetId): string {
    return asset.token || asset.chain || asset.handle;
}

export function patchIn(primary: string, secondary: string,value: string): string {
    return value
        .replace(/\$SYMBOL\(\S+\)/, primary)
        .replace(/\$SYMBOLS\(\S+\)/, primary)
        .replace(/\$PRIMARY/g, primary)
        .replace(/\$SECONDARY/g, secondary)
        .replace(/\$primary/g, primary.toLowerCase())
        .replace(/\$secondary/g, secondary.toLowerCase())
        .replace(/\:date/g, new Intl.DateTimeFormat('sv-SE').format(new Date()));
}

export function patchOut(index: number, path: string, output: any): any {
    const keys = path.replace(/\$/g, index.toString()).split('.');
    for (let i = 0; i < keys.length; i++) {
        const key: any = keys[i];
        output = typeof output == 'object' && !isNaN(parseInt(key)) ? output[Object.keys(output)[key]] : output[key];
        if (output === undefined || output === null)
            throw new Error(`Path '${path}' not found in response`);
    }
    return output;
}

export type QuoteSources = {
    realtime: Record<string, string | [string, Record<string, string>]>;
    fallback: Record<string, string | [string, Record<string, string>]>;
    logging: boolean;
};

export type QuoteResult = {
    value: BigNumber,
    source: 'realtime' | 'fallback' | 'cache'
};

export class Quotes {
    static currencies: string[][] = [
        ["USD", "USDT", "USDC", "DAI"]
    ];
    static quotes: QuoteSources = { realtime: { }, fallback: { }, logging: false };
    static blacklist: Record<string, Set<string>> = { };
    static cache: Record<string, Record<string, { ttl: Date, price: BigNumber }>> = { };
    static offset: number = Math.floor(Math.random() * 65536);

    static setSources(quotes: QuoteSources): void {
        this.quotes = quotes;
    }
    private static async providerPriceOf(source: 'realtime' | 'fallback', primaryAsset: AssetId, secondaryAsset: AssetId): Promise<QuoteResult> {
        const primary = symbolOf(primaryAsset), secondary = symbolOf(secondaryAsset);
        if (primary == secondary)
            return { value: new BigNumber(1), source: 'cache' };

        const pair = primary + secondary;
        try {
            if (!Whitelist.has(primaryAsset))
                throw new Error(`${symbolOf(primaryAsset)} in ${pair} pair requires whitelisting`);
            if (!Whitelist.has(secondaryAsset))
                throw new Error(`${symbolOf(secondaryAsset)} in ${pair} pair requires whitelisting`);
        } catch (exception: any) {
            Log.error(exception.message);
            throw exception
        }

        const sources = this.quotes[source];
        const urls = Object.keys(sources);
        const trials = new Set<string>();
        let bestOldQuote: { ttl: Date, price: BigNumber } | null = null;
        while (trials.size < urls.length) {
            const baseUrl = urls[this.offset++ % urls.length];
            const symbol = baseUrl.match(/\$SYMBOL\((\S+)\)/);
            if (trials.has(baseUrl))
                continue;

            trials.add(baseUrl);
            if (symbol != null && symbol.length >= 2) {
                const base = this.baseOf(secondary);
                if (base != symbol[1])
                    continue;
            }

            const time = new Date();
            let registry = this.cache[baseUrl];
            if (!registry)
                registry = this.cache[baseUrl] = { };
            
            const quote = registry[pair];
            if (quote != null && quote.ttl > time) {
                if (!bestOldQuote || quote.ttl > bestOldQuote.ttl)
                    bestOldQuote = quote;
                continue;
            }
            
            let blacklist = this.blacklist[pair];
            if (blacklist == null)
                blacklist = this.blacklist[pair] = new Set<string>();
            else if (blacklist.has(baseUrl))
                continue;
            
            let networkError = false;
            const patchUrl = patchIn(primary, secondary, baseUrl);
            try {
                const setup = sources[baseUrl];
                const headers = Array.isArray(setup) ? setup[1] : null;
                const path = patchIn(primary, secondary, Array.isArray(setup) ? setup[0] : setup);
                let response: Response;
                try {
                    response = await fetch(new URL(patchUrl), headers ? { headers: headers } : undefined);
                } catch (exception: any) {
                    networkError = exception.message == 'fetch failed' ? (source == 'realtime') : true;
                    throw exception;
                }
                if (!response.ok) {
                    networkError = source == 'realtime' ? true : (response.status == 429 || response.status > 500);
                    throw new Error(response.status + ' - ' + response.statusText);
                }

                const output = await response.json();
                const field = patchOut(0, path, output);
                const price = new BigNumber(field.toString());
                if (price.isNaN() || !price.isFinite() || !price.isGreaterThanOrEqualTo(0))
                    throw new Error(`Invalid price extracted from ${patchUrl}: ${price}`);
                
                if (this.quotes.logging)
                    Log.info(`${source} price query ${patchUrl} for ${pair} pair: ${price.toString()} ${symbolOf(secondaryAsset)}`);

                registry[pair] = { ttl: new Date(time.getTime() + TTL[source]), price: new BigNumber(price) };
                return { value: price, source: source };
            } catch (exception: any) {
                if (!networkError) {
                    Log.error(`${source} price query ${patchUrl} for ${pair} pair error: ${exception.message || 'failed'} (now blacklisted)`);
                    blacklist.add(baseUrl);
                } else {
                    Log.error(`${source} price query ${patchUrl} for ${pair} pair error: ${exception.message || 'failed'}`);
                }
            }
        }

        if (bestOldQuote != null)
            return { value: bestOldQuote.price, source: 'cache' };

        throw new Error(`Price of ${pair} cannot be found: no applicable providers`);
    }
    static async directPriceOf(primaryAsset: AssetId, secondaryAsset: AssetId, source?: 'realtime' | 'fallback'): Promise<QuoteResult> {
        if (source != null)
            return await this.providerPriceOf(source, primaryAsset, secondaryAsset);

        try { return await this.providerPriceOf('realtime', primaryAsset, secondaryAsset); } catch { }
        return await this.providerPriceOf('fallback', primaryAsset, secondaryAsset);
    }
    static async crossPriceOf(primaryAsset: AssetId, secondaryAsset: AssetId): Promise<QuoteResult> {
        try {
            if (!Whitelist.has(primaryAsset))
                throw new Error(`${symbolOf(primaryAsset)} requires whitelisting`);
            if (!Whitelist.has(secondaryAsset))
                throw new Error(`${symbolOf(secondaryAsset)} requires whitelisting`);
        } catch (exception: any) {
            Log.error(exception.message);
            throw exception
        }

        const fetchDirectly = async (primary: AssetId, secondary: AssetId): Promise<QuoteResult> => {
            try { return await this.directPriceOf(primary, secondary); } catch { }
            try {
                const result = await this.directPriceOf(secondary, primary);
                return { value: new BigNumber(1).dividedBy(result.value), source: result.source };
            } catch { }
            throw new Error(`No direct price found for ${symbolOf(primary)}${symbolOf(secondary)}`);
        };
        const fetchIndirectly = async (asset: AssetId): Promise<{ asset: string, price: QuoteResult }> => {
            for (let i = 0; i < this.currencies.length; i++) {
                const currencies = this.currencies[i];
                for (let j = 0; j < currencies.length; j++) {
                    try { return { asset: currencies[0], price: await fetchDirectly(asset, AssetId.fromHandle(currencies[j])) }; } catch { }
                }
            }
            throw new Error(`No indirect price found for ${symbolOf(asset)}`);
        };
        try { return await fetchDirectly(primaryAsset, secondaryAsset); } catch { }

        const primary = await fetchIndirectly(primaryAsset);
        const secondary = await fetchIndirectly(secondaryAsset);
        if (primary.asset == secondary.asset)
            return { value: primary.price.value.dividedBy(secondary.price.value), source: primary.price.source };

        const primaryBase = await fetchIndirectly(AssetId.fromHandle(primary.asset));
        const secondaryBase = await fetchIndirectly(AssetId.fromHandle(secondary.asset));
        if (primaryBase.asset != secondaryBase.asset)
            throw new Error(`No cross price for ${symbolOf(primaryAsset)}${symbolOf(secondaryAsset)} (${primary.asset}${secondary.asset} -> ${primaryBase.asset}${primaryBase.asset})`);

        return { value: primary.price.value.multipliedBy(primaryBase.price.value).dividedBy(secondary.price.value.multipliedBy(secondaryBase.price.value)), source: primary.price.source };
    }
    static globalBase(): string | null {
        const currencies = this.currencies[0];
        return currencies ? currencies[0] || null : null;
    }
    static baseOf(asset: string): string | null {
        const currencies = this.currencies.filter((x) => x.filter((y) => y == asset).length > 0);
        return currencies.length > 0 ? currencies[0][0] : null;
    }
    static assetBaseOf(asset: AssetId): string | null {
        return this.baseOf(symbolOf(asset));
    }
    static isWhitelistingError(exception: any): boolean {
        return exception && typeof exception.message == 'string' && exception.message.indexOf('requires whitelisting');
    }
}