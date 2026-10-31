import BigNumber from 'bignumber.js';
import { AssetId, ByteUtil, Hashing, Uint256 } from "tangentsdk";

export enum MarketPolicy {
    Spot,
    Margin
}

export enum OrderCondition {
    Market,
    Limit,
    Stop,
    StopLimit,
    TrailingStop,
    TrailingStopLimit
}

export enum OrderSide {
    Buy,
    Sell
}

export enum OrderPolicy {
    Deferred,
    DeferredAll,
    Immediate,
    ImmediateAll
}

export enum RefType {
    Order,
    Pool
}

export type Block = {
    blockNumber: number;
    blockHash: Uint256;
}

export enum ContractType {
    DEX = 'dex',
    DLP = 'dlp',
    UAP = 'uap',
    AMS = 'ams'
}

export type DexParameters = {
    poolExitFee: BigNumber;
    maxPoolFeeRate: BigNumber;
    minMakerFee: BigNumber;
    maxMakerFee: BigNumber;
    makerFeeExponent: number;
    minTakerFee: BigNumber;
    maxTakerFee: BigNumber;
    takerFeeExponent: number;
    assetVolumeTarget: BigNumber;
    assetResetDays: number;
    accountResetDays: number;
    marketPolicy: MarketPolicy;
};

export type DlpParameters = {
    rewardEmission: BigNumber;
    rewardBalance: BigNumber;
    permissions: {
        primaryAssetId: Uint256;
        primaryAsset: string;
        secondaryAssetId: Uint256;
        secondaryAsset: string;
    }[];
};

export type AmsParameters = {
    dex?: string;
    feeOverflow: BigNumber;
    bookValidityBlocks: number;
    depositValidityBlocks: number;
    depositPrecision: number;
    sequencer?: string | null;
};

export type UapParameters = { };

export type ContractParameters = DexParameters | DlpParameters | AmsParameters | UapParameters;

export type Contract<T extends ContractParameters = ContractParameters> = {
    id: Uint256;
    type: string;
    accountId: Uint256;
    account?: string;
    deployerAccountId: Uint256 | null;
    deployerAccount?: string | null;
    blockNumber: number | null;
    parameters: T;
    synced: boolean;
}

export type AggregatedPair = {
    id: Uint256,
    primaryAsset: string,
    secondaryAsset: string,
    secondaryBase: string | null,
    launchTime: number,
    poolFeeRate: BigNumber | null,
    price: {
        orderLiquidity: BigNumber | null,
        poolLiquidity: BigNumber | null,
        totalLiquidity: BigNumber | null,
        orderVolume: BigNumber | null,
        poolVolume: BigNumber | null,
        totalVolume: BigNumber | null,
        open: BigNumber | null,
        low: BigNumber | null,
        high: BigNumber | null,
        close: BigNumber | null
    }
}

export type AggregatedTrade = {
    timepoint: number,
    volume: BigNumber,
    open: BigNumber,
    low: BigNumber,
    high: BigNumber,
    close: BigNumber
}

export type AggregatedLog = {
    time: number,
    account: string,
    side: OrderSide | 'lp',
    price: BigNumber,
    quantity: BigNumber
}

export type AggregatedLevel = {
    id: Uint256,
    price: BigNumber,
    quantity: BigNumber,
    curve?: {
        minPrice: BigNumber | null,
        maxPrice: BigNumber | null,
        primaryValue: BigNumber,
        secondaryValue: BigNumber,
        feeRate: BigNumber
    }
}

export type Order = {
    id: Uint256;
    orderId: Uint256;
    pairId: Uint256;
    primaryAsset?: string;
    secondaryAsset?: string;
    accountId: Uint256;
    blockNumber: number;
    condition: OrderCondition;
    side: OrderSide;
    policy: OrderPolicy;
    price?: BigNumber;
    stopPrice?: BigNumber;
    fillingPrice?: BigNumber;
    startingValue: BigNumber;
    value: BigNumber;
    slippage?: BigNumber;
    trailingStep?: BigNumber;
    trailingDistance?: BigNumber;
    lastPrice: BigNumber;
    lastQuantity: BigNumber;
    active: boolean;
}

export type Pool = {
    id: Uint256;
    poolId: Uint256;
    pairId: Uint256;
    primaryAsset?: string;
    secondaryAsset?: string;
    accountId: Uint256;
    blockNumber: number;
    initialPrice: BigNumber;
    initialPrimaryValue: BigNumber;
    initialSecondaryValue: BigNumber;
    primaryValue: BigNumber;
    secondaryValue: BigNumber;
    primaryRevenue: BigNumber;
    secondaryRevenue: BigNumber;
    liquidity: BigNumber;
    price: BigNumber;
    minPrice?: BigNumber;
    maxPrice?: BigNumber;
    volume?: BigNumber;
    feeRate: BigNumber;
    exitFee: BigNumber;
    lastAskPrice: BigNumber;
    lastBidPrice: BigNumber;
    active: boolean;
}

export type Trade = {
    pairId: Uint256;
    makerOrderId?: Uint256;
    makerPoolId?: Uint256;
    makerAccountId?: Uint256;
    takerOrderId?: Uint256;
    takerAccountId?: Uint256;
    blockNumber?: number;
    side: OrderSide;
    price: BigNumber;
    quantity: BigNumber;
    time: Date;
}

export type Depth = {
    pairId: Uint256;
    poolId: Uint256;
    accountId: Uint256;
    blockNumber: number;
    price: BigNumber;
    quantity: BigNumber;
    time: Date;
}

export type DelegatedPool = {
    id: Uint256;
    pairId: Uint256;
    delegatorAccount?: string;
    accountId: Uint256;
    primaryAsset?: string;
    secondaryAsset?: string;
    blockNumber: number;
    rewardValue: BigNumber;
    initialPrimaryValue: BigNumber;
    initialSecondaryValue: BigNumber;
    primaryValue: BigNumber;
    secondaryValue: BigNumber;
    primaryTotal?: BigNumber;
    secondaryTotal?: BigNumber;
    primaryReserve?: BigNumber;
    secondaryReserve?: BigNumber;
    allocationPrice?: BigNumber;
    volume?: BigNumber;
    share?: BigNumber;
    feeRate?: BigNumber;
    active: boolean;
}

export type PseudoDelegatedPool = {
    pairId: Uint256;
    delegatorAccount?: string;
    primaryAsset?: string;
    secondaryAsset?: string;
    initialValue: BigNumber;
    currentValue: BigNumber;
    volume: BigNumber;
    feeRate?: BigNumber;
}

export type PseudoDelegatedState = {
    primaryAsset: string;
    secondaryAsset: string;
    delegatorAccount: string;
    poolId: Uint256;
    primaryLiquidity: BigNumber;
    secondaryLiquidity: BigNumber;
    price: BigNumber;
}

export type Balance = {
    asset: AssetId,
    poly: boolean,
    unavailable: BigNumber,
    available: BigNumber,
    price: BigNumber | null
};

export type Booking = {
    id: Uint256,
    asset: AssetId,
    assetId: Uint256,
    amount: BigNumber,
    publicKey: string,
    status: string,
    expiresBlock: number,
    txHash: string,
    blockNumber: number,
    depositId: string | null,
    depositHash: string | null,
    depositStatus: string | null,
    depositBlock: number | null,
    depositPayer: string | null,
    withdrawHash: string | null,
    bridgeHash: string | null,
    outAsset: AssetId | null,
    outAmount: BigNumber | null,
    broadcastHash: string | null,
    proofTransactionId: string | null,
    relayError: string | null,
    routingAddress: string | null,
    attestateHash: string | null
};

export enum BookingStatus {
    AwaitingDeposit = 'awaiting_deposit',
    FinalizingDeposit = 'finalizing_deposit',
    AwaitingExecution = 'awaiting_execution',
    AwaitingWithdrawal = 'awaiting_withdrawal',
    FinalizingWithdrawal = 'finalizing_withdrawal',
    FailingWithdrawal = 'failing_withdrawal',
    Finalized = 'finalized',
    Expired = 'expired'
}

export type BookingSwap = {
    symbol: string,
    asset: AssetId,
    price: BigNumber,
    amount: BigNumber,
    slippage: BigNumber,
    buying: boolean
};

export type SpendMessageParams = {
    depositHash: Uint256,
    bridgeHash: Uint256,
    inputAsset: AssetId,
    inputAmount: BigNumber,
    contractAddress: string,
    sequencerAddress: string,
    routingAddress: string,
    tokenPath: BookingSwap[],
    feePath: BookingSwap[]
};

export type RespendMessageParams = {
    depositHash: Uint256,
    broadcastHash: Uint256,
    bridgeHash: Uint256,
    asset: AssetId,
    amount: BigNumber,
    contractAddress: string,
    sequencerAddress: string,
    routingAddress: string
};

export type PreattestationEvent = {
    txHash: Uint256,
    txId: string | null,
    payerAccount: string | null,
    asset: AssetId,
    amount: BigNumber,
    source: 'preattestation' | 'mined',
    isSinglePayment: boolean
};

export type BroadcastEvent = {
    txHash: Uint256,
    withdrawHash: Uint256,
    proofTransactionId: string | null,
    error: string | null
};

export type AttestationEvent = {
    txHash: Uint256,
    hashdata: string,
    asset: AssetId | null,
    amount: BigNumber | null
};

export enum PseudoEvent {
    Preattestation = Hashing.hash32(ByteUtil.byteStringToUint8Array('PreattestationEvent')),
    Broadcast = Hashing.hash32(ByteUtil.byteStringToUint8Array('BroadcastEvent')),
    Attestation = Hashing.hash32(ByteUtil.byteStringToUint8Array('AttestationEvent'))
}