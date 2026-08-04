import { Uint256 } from "tangentsdk";

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

export type Market = {
    id: Uint256;
    accountId: Uint256;
    account?: string;
    version?: string;
    deployerAccountId: Uint256;
    deployerAccount?: string;
    blockNumber: number;
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
    marketId: Uint256;
    marketAccount?: string;
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
    marketId: Uint256;
    marketAccount?: string;
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
    marketId?: Uint256;
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
    marketId: Uint256;
    poolId: Uint256;
    accountId: Uint256;
    blockNumber: number;
    price: BigNumber;
    quantity: BigNumber;
    time: Date;
}

export type Delegator = {
    id: Uint256;
    marketId: Uint256;
    accountId: Uint256;
    account?: string;
    deployerAccountId: Uint256;
    deployerAccount?: string;
    blockNumber: number;
    rewardEmission: BigNumber;
    rewardBalance: BigNumber;
    permissions: {
        primaryAssetId: Uint256;
        primaryAsset: string;
        secondaryAssetId: Uint256;
        secondaryAsset: string;
    }[];
}

export type DelegatedPool = {
    id: Uint256;
    pairId: Uint256;
    marketId: Uint256;
    marketAccount?: string;
    delegatorId: Uint256;
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
    marketId: Uint256;
    pairId: Uint256;
    delegatorId: Uint256;
    marketAccount?: string;
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
