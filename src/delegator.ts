import { AssetId, ByteUtil, Chain, Hashsig, LiquidityPool, Messages, Readability, RPC, SchemaUtil, Signing, Spot, Stream, Transactions, Uint256 } from 'tangentsdk';
import { Log } from './logging';
import BigNumber from 'bignumber.js';
import process from 'node:process';
import fs from 'fs';

async function call(target: string, path: string, args: Record<string, any>): Promise<any | null> {
    try {
        const url = new URL(`${target}/${path}`);
        for (let key in args)
            url.searchParams.set(key, args[key]?.toString() || '');

        const response = await fetch(url);
        const result = await response.json() as any;
        return result ? result.result : null;
    } catch {
        return null;
    }
}
async function send(address: string, buildTransaction: (nonce: Uint256, gasPrice: BigNumber, gasLimit: Uint256) => Stream): Promise<string> {
    const gasPrice = new BigNumber((await RPC.getGasPrice(new AssetId(), 0.95))?.price.toString() || 0);
    const nonce = new Uint256((await RPC.getNextAccountNonce(address))?.toString());
    const transaction = buildTransaction(nonce, gasPrice, new Uint256(1_000_000));
    try {
        const receipt = await RPC.simulateTransaction(transaction.encode()); 
        const gasLimit = new Uint256(receipt?.relative_gas_use?.toString() || 0);
        if (!gasLimit.gt(0))
            throw new Error('Failed to simulate transaction');

        const finalizedTransaction = buildTransaction(nonce, gasPrice, gasLimit);
        const transactionHash = await RPC.submitTransaction(finalizedTransaction.encode());
        if (!transactionHash)
            throw new Error('Failed to submit transaction');

        while (true) {
            try {
                const confirmation = await RPC.getTransactionByHash(transactionHash);
                if (!confirmation)
                    throw false;
                break;
            } catch {
                await new Promise((resolve) => setTimeout(resolve, 1_000));
            }
        }

        return transactionHash;
    } catch (exception) {
        Log.info('TX message:', transaction.encode());
        throw exception;
    }
}
async function main() {
    let config;
    BigNumber.config({ DECIMAL_PLACES: 18, ROUNDING_MODE: 1 });
    try {
        config = JSON.parse(fs.readFileSync(process.argv[2]).toString('utf8'));
        if (typeof config != 'object')
            throw new Error('config must be an object');
        
        if (!config.network || !['regtest', 'testnet', 'mainnet'].includes(config.network))
            throw new Error('invalid network');

        if (typeof config.validator != 'string')
            throw new Error('invalid validator url');

        if (typeof config.exchange != 'string')
            throw new Error('invalid exchange');

        Chain.props = (Chain as any)[config.network];
        RPC.applyValidator(config.validator);
    } catch (exception) {
        Log.error('path', process.argv[2] || null, ' failed to load a config:', exception);
        return process.exit(1);
    }

    const secretKey = Signing.decodeSecretKey(typeof config.secretKey == 'string' ? config.secretKey : (process.env[config.secretKeyEnv || 'WSK'] || ''));
    if (!secretKey) {
        Log.error('invalid secret key env');
        return process.exit(1);
    }

    const address = Signing.encodeAddress(Signing.derivePublicKeyHash(Signing.derivePublicKey(secretKey)));
    if (!address) {
        Log.error('invalid address');
        return process.exit(1);
    } else {
        Log.info(`LP delegator account: ${address}`);
    }

    let delegatedPoolsSize: number | null = null;
    const url = config.exchange;
    const checkInterval = config.checkInterval || 1_200;
    const twapInterval = config.twapInterval || 600;
    const threshold = config.treshold || 0.01;
    const feeRate = new BigNumber(config.feeRate || 0.0005);
    const range: number | null = config.range || 0.05;
    while (true) {
        const delegatedPools: {
            primaryAsset: string;
            secondaryAsset: string;
            delegatorAccount: string;
            poolId: string;
            primaryLiquidity: string;
            secondaryLiquidity: string;
            price: string;
        }[] = (await call(url, 'account/delegations', { account: address })) || [];
        if (delegatedPools.length != delegatedPoolsSize) {
            delegatedPoolsSize = delegatedPools.length;
            Log.info(`LP delegations (${delegatedPools.length}):`, delegatedPools);
        }
        for (let i = 0; i < delegatedPools.length; i++) {
            let maybePoolId: string | null = null;
            const delegatedPool = delegatedPools[i];
            try {
                const poolId = maybePoolId = new BigNumber(delegatedPool.poolId).gt(0) ? delegatedPool.poolId : '(null)';      
                const primaryReserve = new BigNumber(delegatedPool.primaryLiquidity);
                const secondaryReserve = new BigNumber(delegatedPool.secondaryLiquidity);
                if (!primaryReserve.gt(0) || !secondaryReserve.gt(0)) {
                    Log.info(`LP ${poolId} skipped: no ${primaryReserve.gt(0) ? 'secondary' : (secondaryReserve.gt(0) ? 'primary' : 'primary/secondary')} liquidity (${i + 1}/${delegatedPools.length})`);
                    continue;
                }
                
                const primaryAsset = new AssetId(delegatedPool.primaryAsset);
                const secondaryAsset = new AssetId(delegatedPool.secondaryAsset);
                const prevPrice = new BigNumber(delegatedPool.price || '0');
                const price = new BigNumber((await call(url, 'market/price', { primaryAssetHash: primaryAsset.id, secondaryAssetHash: secondaryAsset.id, interval: twapInterval })) || '0');
                const delta = prevPrice.gt(0) ? (price ? price : prevPrice).minus(prevPrice).dividedBy(prevPrice).abs() : new BigNumber(Math.max(threshold, 1))
                if (!price.gt(0)) {
                    Log.info(`LP ${poolId} skipped: no market price (${i + 1}/${delegatedPools.length})`);
                    continue;
                } else if (!delta.gt(threshold)) {
                    Log.info(`LP ${poolId} passed: ${ByteUtil.bigNumberToString(price as any)} +${delta.multipliedBy(100).toFixed(2)}% dev (${i + 1}/${delegatedPools.length})`);
                    continue;
                } else {
                    Log.info(`LP ${poolId} staled: ${ByteUtil.bigNumberToString(prevPrice as any)} +${delta.multipliedBy(100).toFixed(2)}% dev (${i + 1}/${delegatedPools.length})`);
                }
                
                const priceRange = range ? LiquidityPool.toRange(primaryReserve, secondaryReserve, price, range) : null;
                const minPrice = priceRange?.minPrice || null, maxPrice = priceRange?.maxPrice || null;
                let secondaryValue = LiquidityPool.toSecondaryValue(primaryReserve, price, minPrice, maxPrice);
                if (!secondaryValue)
                    throw new Error('Insufficient primary reserve');

                let primaryValue: BigNumber | null = primaryReserve;
                if (secondaryValue.gt(secondaryReserve)) {
                    console.log('GT THAN RESERVE')
                    secondaryValue = secondaryReserve;
                    primaryValue = LiquidityPool.toPrimaryValue(secondaryReserve, price, minPrice, maxPrice);
                    if (!primaryValue)
                        throw new Error('Insufficient secondary reserve');
                }

                primaryValue = BigNumber.min(primaryReserve, primaryValue);
                secondaryValue = BigNumber.min(secondaryReserve, secondaryValue);
                const transactionHash = await send(address, (nonce: Uint256, gasPrice: BigNumber, gasLimit: Uint256) => {
                    const transaction = {
                        signature: new Hashsig(),
                        asset: new AssetId(),
                        nonce: nonce,
                        gasPrice: gasPrice,
                        gasLimit: gasLimit,
                        callable: Signing.decodeAddress(delegatedPool.delegatorAccount),
                        pays: [],
                        function: Readability.toFunction(Spot.DLP.transferLiquidity),
                        args: [primaryAsset.toUint256(), secondaryAsset.toUint256(), primaryValue, secondaryValue, price, range ? minPrice : new BigNumber(-1), range ? maxPrice : new BigNumber(-1), feeRate]        
                    };
                    
                    let stream = new Stream();
                    SchemaUtil.store(stream, transaction, Messages.asSigningSchema(new Transactions.Call()));
                    const signature = Signing.sign(stream.hash(), secretKey);
                    if (!signature)
                        throw new Error('Failed to sign a transaction');

                    stream = new Stream();
                    SchemaUtil.store(stream, { ...transaction, signature: signature }, new Transactions.Call());
                    return stream;
                });
                Log.info(`LP ${poolId} renewal finalized (price: ${price.toString()}, tx: ${transactionHash})`);
            } catch (exception) {
                Log.error(`LP ${maybePoolId || '(null)'} renewal failed:`, exception);
                process.exit(1);
            }
        }

        Log.info(`LP cycle finalized (next: ${new Date(new Date().getTime() + checkInterval * 1_000)})`);
        await new Promise((resolve) => setTimeout(resolve, checkInterval * 1_000));
    }
}

main();