import { AssetId, Chain, DEX, Pubkeyhash, RPC, Signing, Types, Uint256 } from "tangentsdk";
import { Log } from "./../logging";
import { Exchange } from "./exchange";
import { BigNumber } from "bignumber.js";

export type Options = {
    validator?: string;
    markets?: Record<string, string>;
    network?: 'regtest' | 'testnet' | 'mainnet'
}

export type Events = {
    orderEvent: number;
    tradeEvent: number;
}

export type BlockchainInfo = AssetId & {
    divisibility: BigNumber,
    transactionFinality: BigNumber,
    transactionExpires: boolean,
    compositionPolicy: string,
    tokenPolicy: string,
    routingPolicy: string
}

export type TransactionInfo = {
    hash: string;
    fromAccount: string;
    toAccount: string;
    method: string;
    args: any[],
    pays: { asset: AssetId, value: BigNumber }[];
    events: { type: number, args: any[] }[];
};

export class Blockchain {
    static markets: {
        versions: Record<string, string>,
        accounts: string[]
    } = { versions: { }, accounts: [] };
    static blockchains: BlockchainInfo[] | null = null;
    static genesisBlockNumber: number | null = null;
    static timer: number | null = null;
    static syncing: boolean;

    static async setup(config: Options): Promise<void> {
        if (config.network && ['regtest', 'testnet', 'mainnet'].includes(config.network))
            Chain.props = Chain[config.network];

        if (typeof config.markets == 'object') {
            const targets = config.markets;
            this.markets.accounts = Object.keys(targets).map(x => targets[x]);
            this.markets.versions = { };
            for (let version in targets) {
                this.markets.versions[targets[version]] = version;
            }
        }
        RPC.applyValidator(config.validator || null);
        RPC.applyImplementation({
            onNodeMessage: undefined,
            onNodeRequest: (method: string, message: any, _: number) => Log.query(`blockchain call (method: ${method}):`, message),
            onNodeResponse: (method: string, message: any, _: number) => Log.query(`blockchain return (method: ${method}):`, message),
            onNodeError: (method: string, error: unknown) => Log.error(`blockchain call ${method}: ${(error as any)?.message || error}`)
        });
        return await this.reconfigure();
    }
    static async shutdown(): Promise<void> {
        if (this.timer != null) {
            clearTimeout(this.timer);
            this.timer = null;
        }
        if (RPC.socket != null)
            await RPC.disconnectSocket();
    }
    static async reconfigure(): Promise<void> {
        await this.shutdown();
        if (this.markets.accounts.length > 0) {
            RPC.applyTopics(this.markets.accounts, true);
            const result = await RPC.connectSocket();
            if (!result)
                throw new Error('rpc connection failed');

            this.timer = setInterval(() => RPC.connectSocket(), 15000) as any;
        }
    }
    static async sync(): Promise<void> {
        if (!this.markets.accounts.length || this.syncing)
            return;

        if (this.genesisBlockNumber == null) {
            for (let i = 0; i < this.markets.accounts.length; i++) {
                const transaction = await RPC.getTransactionsByOwner(this.markets.accounts[i], 0, 1, -1, 2);
                if (Array.isArray(transaction) && transaction.length > 0) {
                    this.genesisBlockNumber = Math.min(parseInt(transaction[0].receipt.block_number.toString()), this.genesisBlockNumber || Number.MAX_SAFE_INTEGER);
                }
            }
        }

        this.syncing = true;
        try {
            let tip = new BigNumber(await RPC.getBlockTipNumber() || 0);
            let nextBlock = await Exchange.getLatestBlock();
            let reorganize = (nextBlock?.blockNumber || 0) > tip.toNumber();
            if (reorganize) {
                nextBlock = await Exchange.getBlockByNumber(tip.toNumber());
            }

            if (nextBlock != null) {
                let collisionBlock = await this.findBlock(nextBlock?.blockHash);
                while (nextBlock != null && nextBlock.blockNumber > 0 && collisionBlock == null) {
                    nextBlock = nextBlock.blockNumber > 1 ? await Exchange.getBlockByNumber(nextBlock.blockNumber - 1) : null;
                    if (nextBlock != null)
                        collisionBlock = await this.findBlock(nextBlock?.blockHash);
                    reorganize = true;
                }

                const rollbackBlockNumber = nextBlock?.blockNumber || 0;
                if (reorganize) {
                    Log.info(`blockchain reorganize: ${rollbackBlockNumber > 0 ? 'rollback to block ' + rollbackBlockNumber.toString() : 'rebuild from scratch'} (collision: ${nextBlock ? nextBlock.blockHash.toHex() : 'null'})`);
                    await Exchange.rollbackToBlock(rollbackBlockNumber);
                }
            }
            
            let nextTip = BigNumber.max(new BigNumber((nextBlock?.blockNumber || 0) + 1), new BigNumber(this.genesisBlockNumber || 1));
            let baseTip = new BigNumber(nextTip);
            while (nextTip.lte(tip)) {
                await this.dispatchBlock(nextTip.toNumber());
                Log.info(`blockchain sync progress: ${nextTip.eq(tip) ? '100.00' : nextTip.minus(baseTip).dividedBy(tip.minus(baseTip)).multipliedBy(100).toFixed(2)}% (block: ${nextTip.toString()})`);
                nextTip = nextTip.plus(1);
            }
        } catch (exception) {
            Log.error('blockchain sync failed:', exception);
        }
        
        RPC.onNodeMessage = async (event) => {
            if (event.type == 'block' && typeof event.result.number == 'number' && event.result.number > 0) {
                await this.sync();
            }
        };
        this.syncing = false;
    }
    static async call(toAccount: string, method: string, args: any[]): Promise<any> {
        const asset = AssetId.fromHandle('BTC');
        const fromAccount = Signing.encodeAddress(new Pubkeyhash('0xFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF')) || '';
        const receipt = await RPC.callTransaction(asset, fromAccount, toAccount, method, args);
        if (!receipt || !receipt.successful)
            throw new Error(`call ${toAccount}.${method} error`);
        return receipt.result;
    }
    static async getAccountBalances(account: string): Promise<{ asset: string, value: BigNumber }[]> {
        const balances = (await RPC.fetchAll((offset, count) => RPC.getAccountBalances(account, offset, count))) || [];
        return balances.map((v) => { 
            return {
                asset: v.asset.id,
                value: v.balance
            }
        });
    }
    static async getBlockchains(): Promise<BlockchainInfo[]> {
        if (this.blockchains != null)
            return this.blockchains;

        const results = await RPC.getBlockchains();
        if (!results)
            return [];

        return results.map((v) => {
            const asset = new AssetId(v.id);
            return {
                ...asset,
                divisibility: new BigNumber(v.divisibility),
                transactionFinality: new BigNumber(v.transaction_finality),
                transactionExpires: v.transaction_expires,
                compositionPolicy: v.composition_policy,
                tokenPolicy: v.token_policy,
                routingPolicy: v.routing_policy
            } as any
        });
    }
    static async getAssetHolders(asset: AssetId): Promise<number | null> {
        return RPC.getAssetHolders(asset, 0);
    }
    static async dispatchBlock(number: number): Promise<void> {
        const transactions = await RPC.getBlockTransactionsByNumber(number, 3);
        let accounts: string[] = [], matches = 0;
        let results: Record<string, TransactionInfo[]> = { };
        if (Array.isArray(transactions)) {
            for (let i = 0; i < transactions.length; i++) {
                const result = transactions[i];
                const receipt = result.receipt;
                if (!receipt.successful)
                    continue;

                if (result.affected && Array.isArray(result.affected.accounts))
                    accounts = accounts.concat(result.affected.accounts);

                const subtransactions: { transaction: any, sender: string, events: any[] }[] = [];
                if (result.transaction.type == 'rollup' && Array.isArray(result.transaction.transactions)) {
                    for (let j = 0; j < result.transaction.transactions.length; j++) {
                        const subtransaction = result.transaction.transactions[j];
                        const subhash = new Uint256(subtransaction.action.hash);
                        const eventsBegin = receipt.events.findIndex((v: any) => v.event == Types.Rollup && new Uint256(v.args[0]).eq(subhash));
                        if (eventsBegin != -1) {
                            const eventsEnd = receipt.events.findIndex((v: any, index: number) => index > eventsBegin && v.event == Types.Rollup);
                            subtransactions.push({
                                transaction: subtransaction.action,
                                sender: subtransaction.signer || receipt.from,
                                events: receipt.events.slice(eventsBegin, eventsEnd == -1 ? receipt.events.length : eventsEnd)
                            });
                        }
                    }
                } else {
                    subtransactions.push({
                        transaction: result.transaction,
                        sender: receipt.from,
                        events: receipt.events
                    });
                }

                for (let j = 0; j < subtransactions.length; j++) {
                    const target = subtransactions[j];
                    const toAccount = target.transaction.callable || '';
                    if ((target.transaction.type != 'call' && target.transaction.type != 'deploy') || this.markets.accounts.indexOf(toAccount) == -1)
                        continue;
                    
                    const filteredEvents = [];
                    for (let i = 0; i < target.events.length; i++) {
                        const item = target.events[i];
                        const event = BigNumber.isBigNumber(item.event) ? item.event.toNumber() : parseInt(item.event);
                        if (dispatchableEvents.indexOf(event) != -1) {
                            filteredEvents.push({ type: event, args: item.args });
                        }
                    }
                    
                    const subresult = {
                        hash: target.transaction.hash || '',
                        fromAccount: target.sender || '',
                        toAccount: toAccount,
                        method: target.transaction.function || DEX.Spot.construct,
                        args: target.transaction.args || [],
                        pays: target.transaction.pays || [],
                        events: filteredEvents
                    };
                    const subresults = results[subresult.toAccount];
                    if (subresults != null)
                        subresults.push(subresult);
                    else
                        results[subresult.toAccount] = [subresult];
                    ++matches;
                }
            }
        }

        const block = await RPC.getBlockByNumber(number);
        const blockTime = new BigNumber(block.generation_time);
        const blockDate = blockTime.isNaN() ? new Date() : new Date(blockTime.toNumber());
        const blockHash = new Uint256(block.hash, 16);
        await Exchange.setBlock({ blockNumber: number, blockHash: blockHash }, accounts);
        for (let account in results) {
            try {
                await Exchange.dispatchTransactions(number, blockDate, account, results[account]);
            } catch (exception) {
                Log.error(`failed to dispatch block events (block: ${number}, account: ${account}):`, exception);
            }
        }
    }
    private static async findBlock(hash?: Uint256): Promise<any> {
        try {
            return await RPC.forcedPolicy('no-cache', () => RPC.getBlockByHash(hash?.toHex() || ''));
        } catch {
            return null;
        }
    }
}

export const dispatchableEvents: number[] = [
    DEX.Spot.Events.Config,
    DEX.Spot.Events.Order,
    DEX.Spot.Events.Pool,
    DEX.Spot.Events.Swap,
    DEX.Spot.Events.AssetTier
];