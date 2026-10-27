import { AssetId, Chain, Spot, Pubkeyhash, RPC, Signing, Types, Uint256 } from "tangentsdk";
import { Log } from "./../logging";
import { Exchange } from "./exchange";
import { Common } from "./../common";
import { AttestationEvent, BroadcastEvent, ContractType, PreattestationEvent, PseudoEvent } from "./../types";
import { BigNumber } from "bignumber.js";

export type Options = {
    validator?: string;
    contracts?: Record<string, string>;
    network?: 'regtest' | 'testnet' | 'mainnet';
    tip?: number;
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

export type EventInfo = {
    hash: string;
    fromAccount: string;
    toAccount: string;
    method: string | null;
    args: any[],
    pays: { asset: AssetId, value: BigNumber }[];
    event: { type: number, args: any[] };
};

export class Blockchain {
    static contracts: Partial<Record<ContractType, string>> = { };
    static blockchains: BlockchainInfo[] | null = null;
    static timer: number | null = null;
    static syncing: boolean;
    static blockListeners: ((number: number) => void)[] = [ ];
    static addBlockListener(callback: (number: number) => void): () => void {
        this.blockListeners.push(callback);
        return () => this.removeBlockListener(callback);
    }
    static removeBlockListener(callback: (number: number) => void): void {
        const index = this.blockListeners.indexOf(callback);
        if (index != -1)
            this.blockListeners.splice(index, 1);
    }
    private static fireBlock(number: number): void {
        for (let listener of this.blockListeners.slice()) {
            try {
                listener(number);
            } catch { }
        }
    }

    static accountOf(account: string): ({ account: string, type: ContractType }) | null {
        for (let type in this.contracts)
            if (this.contracts[type as ContractType] == account)
                return { account: account, type: type as ContractType };

        return null;
    }
    static async setup(config: Options): Promise<void> {
        if (config.network && ['regtest', 'testnet', 'mainnet'].includes(config.network))
            Chain.props = Chain[config.network];

        if (typeof config.contracts == 'object') {
            this.contracts = { };
            for (let type in config.contracts) {
                const account = config.contracts[type];
                if (typeof account == 'string' && account.length > 0 && Object.values(ContractType).includes(type as ContractType))
                    this.contracts[type as ContractType] = account;
            }
        }
        RPC.applyValidator(config.validator || null);
        RPC.applyImplementation({
            onNodeMessage: (method: string, message: { args: any; error: unknown; } | { args: any; result: any; }, _: number) => Log.query(`blockchain rpc`, method, message),
        });
        RPC.onNodeEvent = async (event) => {
            if (event.type == 'block' && typeof event.result.number == 'number' && event.result.number > 0) {
                this.fireBlock(event.result.number);
                await this.sync(event.result.number);
            }
        };
        return await this.keepAlive(true);
    }
    static async shutdown(): Promise<void> {
        if (this.timer != null) {
            clearTimeout(this.timer);
            this.timer = null;
        }
        if (RPC.socket != null)
            await RPC.disconnectSocket();
    }
    static async keepAlive(healthCheck: boolean = false, hadConnection: boolean = false): Promise<void> {
        try {
            if (!RPC.socket || !hadConnection) {
                await RPC.unsubscribeTopics();
                const result = await RPC.subscribeTopics(Object.values(this.contracts), true);
                if (!RPC.socket || result == null) {
                    throw new Error('rpc unreachable');
                } else if (!hadConnection) {
                    Log.info('blockchain rpc acquired (topics: ' + (result != null ? result.toString() : 'null') + ')');
                    hadConnection = true;
                }
            }
        } catch (exception) {
            Log.error('blockchain rpc connection error:', exception);
            hadConnection = false;
        }
        if (healthCheck) {
            this.timer = setTimeout(() => this.keepAlive(healthCheck, hadConnection), 5000) as any;
        }
    }
    static async sync(tipBlockNumber?: number): Promise<void> {
        const addresses = Object.values(this.contracts);
        if (!addresses.length || this.syncing)
            return;

        this.syncing = true;
        try {
            let pendingQueue: number[] = [], syncedBlockNumber = (await Exchange.getLatestBlock())?.blockNumber || null;
            while (true) {
                let boundaryBlock: number | null = null;
                const queue: number[] = [];
                while (true) {
                    const transactions = await RPC.getTransactionsByOwner(addresses, queue.length, queue.length > 0 ? 32 : 1, 0, 2);
                    if (!Array.isArray(transactions) || !transactions.length)
                        break;

                    let finalize = false;
                    for (let i = 0; i < transactions.length && !finalize; i++) {
                        const blockNumber = parseInt(transactions[i].receipt.block_number.toString());
                        if (syncedBlockNumber && blockNumber <= syncedBlockNumber) {
                            if (boundaryBlock == null || blockNumber > boundaryBlock)
                                boundaryBlock = blockNumber;
                            finalize = true;
                        }
                        if (!finalize)
                            queue.unshift(blockNumber);
                    }

                    if (finalize)
                        break;
                }

                if (queue.length > 0)
                    Log.info(`blockchain indexing: ${queue.length} transaction${queue.length > 1 ? 's' : ''}`);

                pendingQueue = queue;
                if (boundaryBlock == null)
                    break;

                const stored = await Exchange.getBlockByNumber(boundaryBlock);
                if (stored != null && await this.findBlock(stored.blockHash) != null)
                    break;

                const forkPoint = stored ? await this.resolveForkPoint(boundaryBlock) : 0;
                Log.info(`blockchain reorganize: ${forkPoint > 0 ? 'rollback to block ' + forkPoint : 'rebuild from scratch'} (boundary: ${boundaryBlock}, collision: ${stored ? stored.blockHash.toHex() : 'null'})`);
                await Exchange.rollbackToBlock(forkPoint);
                syncedBlockNumber = forkPoint > 0 ? forkPoint : null;
            }

            if (!pendingQueue.length) {
                const tipNumber = tipBlockNumber != null ? tipBlockNumber : await RPC.getBlockTipNumber().then((v) => v != null ? parseInt(v.toString()) : null).catch(() => null);
                if (syncedBlockNumber != null && tipNumber != null && syncedBlockNumber > tipNumber) {
                    const forkPoint = await this.resolveForkPoint(syncedBlockNumber);
                    Log.info(`blockchain reorganize: ${forkPoint > 0 ? 'rollback to block ' + forkPoint : 'rebuild from scratch'} (rewind: stored ${syncedBlockNumber} > tip ${tipNumber})`);
                    await Exchange.rollbackToBlock(forkPoint);
                    syncedBlockNumber = forkPoint > 0 ? forkPoint : null;
                }
            }

            pendingQueue.sort((a: any, b: any) => a - b);
            for (let i = 0; i < pendingQueue.length; i++) {
                const blockNumber = pendingQueue[i];
                await this.dispatchBlock(blockNumber);
                Log.info(`blockchain sync progress: ${(100 * (i + 1) / pendingQueue.length).toFixed(2)}% (block: ${blockNumber})`);
            }

            if (tipBlockNumber && !pendingQueue.find(x => x == tipBlockNumber)) {
                await this.dispatchBlock(tipBlockNumber);
                Log.info(`blockchain sync: OK complete (block: ${tipBlockNumber}, transactions: ${pendingQueue.length})`);
            } else {
                Log.info(`blockchain sync: OK complete (transactions: ${pendingQueue.length})`);
            }
        } catch (exception) {
            Log.error('blockchain sync failed:', exception);
        }
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
        const block = await RPC.getBlockByNumber(number, 3);
        if (block == null) {
            Log.info(`blockchain block unavailable (block: ${number}), deferred to next sync`);
            return;
        }

        const transactions = block.transactions;
        const topics = Object.values(this.contracts);
        let accounts: string[] = [], matches = 0;
        let results: Record<string, EventInfo[]> = { };
        const preattestations: Record<string, PreattestationEvent[]> = { };
        const broadcasts: BroadcastEvent[] = [];
        const attestates: AttestationEvent[] = [];
        if (Array.isArray(transactions)) {
            for (let i = 0; i < transactions.length; i++) {
                const result = transactions[i];
                const receipt = result.receipt;
                if (!receipt.successful)
                    continue;

                if (result.affected && Array.isArray(result.affected.accounts))
                    accounts = accounts.concat(result.affected.accounts);
                if (result.transaction.type == 'broadcast' || result.transaction.type == 'attestate') {
                    const txHash = Common.u256(result.transaction.hash);
                    const proof = result.transaction['proof'];
                    if (result.transaction.type == 'attestate') {
                        const hashdata = String((proof != null && typeof proof == 'object' ? proof['hashdata'] ?? proof['hash_data'] ?? proof['transaction_id'] ?? proof['transactionId'] : null) ?? result.transaction['hashdata'] ?? result.transaction['hash_data'] ?? '');
                        if (txHash != null && hashdata.length > 0)
                            attestates.push({ txHash: txHash, hashdata: hashdata });
                    } else {
                        const withdrawHash = Common.u256(result.transaction['withdraw_hash'] ?? null);
                        if (txHash != null && withdrawHash != null) {
                            const proofTransactionId = String((proof != null && typeof proof == 'object' ? proof['hashdata'] ?? proof['hash_data'] ?? proof['transaction_id'] ?? proof['transactionId'] : null) ?? result.transaction['hashdata'] ?? result.transaction['hash_data'] ?? '');
                            const relayError = typeof result.transaction['error'] == 'string' && result.transaction['error'].length > 0 ? String(result.transaction['error']) : null;
                            broadcasts.push({ txHash: txHash, withdrawHash: withdrawHash, proofTransactionId: proofTransactionId.length > 0 ? proofTransactionId : null, error: relayError });
                        }
                    }
                }

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
                    const toAccount = target.transaction.callable || target.transaction.recipient || '';
                    const payments = this.transactionPayments(target.transaction);
                    if (payments != null) {
                        const depositHash = Common.u256(target.transaction.hash);
                        if (depositHash != null) {
                            for (let recipient in payments) {
                                if (this.contracts[ContractType.AMS] != recipient)
                                    continue;

                                const rows = this.preattestationEvents(payments[recipient], depositHash, target.sender || null, 'mined');
                                if (rows.length > 0) {
                                    const subdeposits = preattestations[recipient] || (preattestations[recipient] = []);
                                    for (let k = 0; k < rows.length; k++)
                                        subdeposits.push(rows[k]);
                                }
                            }
                        }
                    }

                    if (target.transaction.type != 'call' && target.transaction.type != 'deploy')
                        continue;

                    const events = target.events.map((x) => ({
                        hash: target.transaction.hash || '',
                        fromAccount: target.sender || '',
                        toAccount: x.emitter || toAccount,
                        method: target.transaction.function || null,
                        args: target.transaction.args || [],
                        pays: target.transaction.pays || [],
                        event: {
                            type: BigNumber.isBigNumber(x.event) ? x.event.toNumber() : parseInt(x.event),
                            args: x.args
                        }
                    })).filter((x) => topics.indexOf(x.toAccount) != -1 && dispatchableEvents.indexOf(x.event.type) != -1);
                    for (let i = 0; i < events.length; i++) {
                        const event = events[i];
                        const subresults = results[event.toAccount];
                        if (subresults != null)
                            subresults.push(event);
                        else
                            results[event.toAccount] = [event];
                        ++matches;
                    }
                }
            }
        }

        const blockTime = new BigNumber(block.generation_time);
        const blockDate = blockTime.isNaN() ? new Date() : new Date(blockTime.toNumber());
        const blockHash = new Uint256(block.hash, 16);
        const blockRef = { number: number, time: blockDate };
        await Exchange.setBlock({ blockNumber: number, blockHash: blockHash }, accounts);
        
        const amsAccount = this.contracts[ContractType.AMS];
        if (amsAccount != null) {
            const deposits: EventInfo[] = [];
            for (let account in preattestations) {
                for (let i = 0; i < preattestations[account].length; i++) {
                    const row = preattestations[account][i];
                    deposits.push(this.amsIndexedEvent(PseudoEvent.Preattestation, row.txHash.toHex(), account, [row]));
                }
            }
            const facts: EventInfo[] = [];
            for (let i = 0; i < broadcasts.length; i++)
                facts.push(this.amsIndexedEvent(PseudoEvent.Broadcast, broadcasts[i].txHash.toHex(), amsAccount, [broadcasts[i]]));
            for (let i = 0; i < attestates.length; i++)
                facts.push(this.amsIndexedEvent(PseudoEvent.Attestation, attestates[i].txHash.toHex(), amsAccount, [attestates[i]]));
            if (deposits.length > 0 || facts.length > 0)
                results[amsAccount] = [...deposits, ...(results[amsAccount] ?? []), ...facts];
        }

        for (let account in results) {
            try {
                const accountRef = this.accountOf(account);
                if (accountRef != null) {
                    await Exchange.dispatchEvents(blockRef, accountRef, results[account]);
                }
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
    private static async resolveForkPoint(upperBound: number): Promise<number> {
        let lo = 0, hi = upperBound;
        while (hi - lo > 1) {
            const mid = Math.floor((lo + hi) / 2);
            if (await this.blockMatchesNode(mid))
                lo = mid;
            else
                hi = mid;
        }

        return lo;
    }
    private static async blockMatchesNode(blockNumber: number): Promise<boolean> {
        const stored = await Exchange.getBlockByNumber(blockNumber);
        if (stored == null)
            return false;

        try {
            const block = await RPC.forcedPolicy('no-cache', () => RPC.getBlockByNumber(blockNumber));
            return block != null && new Uint256(String(block.hash ?? '')).toHex() == stored.blockHash.toHex();
        } catch {
            return false;
        }
    }
    static async pollPreattestations(count: number = 64): Promise<void> {
        const amsAccount = this.contracts[ContractType.AMS];
        if (amsAccount == null)
            return;

        const result = await RPC.getMempoolPreattestations(0, count, { transactionIds: [], addresses: [amsAccount], memoAccounts: [] });
        const items: any[] = Array.isArray(result) ? result : (result != null && typeof result == 'object' && Array.isArray(result['transactions']) ? result['transactions'] : []);
        const preattestations: Record<string, PreattestationEvent[]> = { };
        for (let i = 0; i < items.length; i++) {
            const entry = items[i];
            if (entry == null || typeof entry != 'object')
                continue;

            const transaction = entry['transaction'] != null && typeof entry['transaction'] == 'object' ? entry['transaction'] : entry;
            if (entry['successful'] === false || transaction['successful'] === false)
                continue;

            const payments = this.transactionPayments(transaction);
            if (payments == null)
                continue;

            const depositHash = Common.u256(entry['transactionId'] ?? transaction['hash'] ?? entry['hash'] ?? null);
            if (depositHash == null)
                continue;

            const payerAccount = typeof transaction['from'] == 'string' ? transaction['from'] : (typeof transaction['fromAccount'] == 'string' ? transaction['fromAccount'] : null);
            for (let recipient in payments) {
                if (this.contracts[ContractType.AMS] != recipient)
                    continue;

                const rows = this.preattestationEvents(payments[recipient], depositHash, payerAccount, 'preattestation');
                if (rows.length > 0) {
                    const subdeposits = preattestations[recipient] || (preattestations[recipient] = []);
                    for (let j = 0; j < rows.length; j++)
                        subdeposits.push(rows[j]);
                }
            }
        }

        const top = await Exchange.getLatestBlock();
        for (let account in preattestations) {
            try {
                const events: EventInfo[] = [];
                for (let i = 0; i < preattestations[account].length; i++)
                    events.push(this.amsIndexedEvent(PseudoEvent.Preattestation, preattestations[account][i].txHash.toHex(), account, [preattestations[account][i]]));
                await Exchange.dispatchEvents({ number: top?.blockNumber ?? 0, time: new Date() }, { account: account, type: ContractType.AMS }, events);
            } catch (exception) {
                Log.error('failed to dispatch ams preattestation deposits:', exception);
            }
        }
    }
    private static transactionPayments(transaction: unknown): Record<string, unknown[]> | null {
        if (transaction == null || typeof transaction != 'object')
            return null;

        const tx = transaction as Record<string, unknown>;
        let result: Record<string, unknown[]> | null = null;
        const remember = (address: string, payment: unknown) => {
            if (address.length < 1 || payment == null)
                return;

            if (result == null)
                result = { };

            (result[address] || (result[address] = [])).push(payment);
        };

        if (Array.isArray(tx.pays)) {
            const recipient = String(tx.callable ?? tx.recipient ?? '');
            for (let i = 0; i < tx.pays.length; i++)
                remember(recipient, tx.pays[i]);
        }

        if (tx.type == 'transfer' && tx.asset != null) {
            const targets = Array.isArray(tx.to) ? tx.to : [{ to: tx.to, value: tx.value }];
            for (let i = 0; i < targets.length; i++) {
                const entry = targets[i];
                if (entry != null && typeof entry == 'object' && 'to' in entry && 'value' in entry)
                    remember(String(entry.to), { asset: tx.asset, value: entry.value });
            }
        }

        return result;
    }
    private static amsIndexedEvent(type: PseudoEvent, hash: string, toAccount: string, args: any[]): EventInfo {
        return { hash: hash, fromAccount: '', toAccount: toAccount, method: null, args: [], pays: [], event: { type: type, args: args } };
    }
    private static preattestationEvents(pays: any[], depositHash: Uint256, payerAccount: string | null, source: 'preattestation' | 'mined'): PreattestationEvent[] {
        const totals: Record<string, BigNumber> = { };
        const bridged: Record<string, { asset: AssetId, amount: BigNumber }> = { };
        for (let i = 0; i < pays.length; i++) {
            try {
                const raw = pays[i].asset;
                let id: unknown = null;
                if (typeof raw == 'string')
                    id = raw;
                else if (raw != null && typeof raw == 'object' && 'id' in raw)
                    id = raw.id;
                if (typeof id != 'string' || id.length < 1)
                    continue;

                const asset = new AssetId(id);
                if (!asset.isValid())
                    continue;

                const value = new BigNumber(pays[i].value ?? pays[i].amount);
                if (value.isNaN() || value.isLessThanOrEqualTo(0))
                    continue;

                totals[asset.id] = (totals[asset.id] || new BigNumber(0)).plus(value);
                if (asset.chain != null && asset.chain.length > 0)
                    bridged[asset.id] = { asset: asset, amount: (bridged[id]?.amount || new BigNumber(0)).plus(value) };
            } catch { }
        }

        const isSinglePayment = Object.keys(totals).length == 1;
        const rows: PreattestationEvent[] = [];
        for (let id in bridged)
            rows.push({ txHash: depositHash, payerAccount: payerAccount, asset: bridged[id].asset, amount: bridged[id].amount, source: source, isSinglePayment: isSinglePayment });
        return rows;
    }
}

export const dispatchableEvents: number[] = [
    Spot.DEX.Events.Config,
    Spot.DEX.Events.Order,
    Spot.DEX.Events.Pool,
    Spot.DEX.Events.Swap,
    Spot.DEX.Events.AssetTier,
    Spot.DLP.Events.Config,
    Spot.DLP.Events.PoolRefEvent,
    Spot.AMS.Events.Config,
    Spot.AMS.Events.BookingRef,
    Spot.AMS.Events.SwapRef,
    Spot.AMS.Events.SpentBooking,
];