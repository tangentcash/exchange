// npx tsc src/abi/.test/spot.ams.data.ts --module commonjs --target es2020 --esModuleInterop --skipLibCheck --strict false
// node src/abi/.test/spot.ams.data.js
import { Signing, Uint256, AssetId, Pubkeyhash, Pubkey, Seckey, ByteUtil, Stream, Chain } from "tangentsdk";
import { blake2b } from "@noble/hashes/blake2b";
import BigNumber from "bignumber.js";

const AMS = "tcrt1xqvvsy3ayfp8w7fs4wg3pt5y6r69c56jjuf87ys";
const DEX = "tcrt1xn58mly6f7er0zs6swtetqgfqs36jaarq7yd7gc";
const SPENDER = "tcrt1x25x782mml6cty0xg6hfmvukqgmp9fq09d9v9av";
const BTC_DEPOSIT = "676ec08bc77396b14eb11715947487c7eb76a9e941034717f6273bf5b7efe868";
const BROADCAST = "68bfaaee57858c49740c4d51214fc38443722aee19560cf46a23614b98adb81e";
const ETH_DEPOSIT = "33858336582242677103876590952323141828732928068020152863387286025527637439362";
const BTC_BRIDGE = "2f4770f43fd62891e453ed12858fbf00508814387aa8fd5dfcba7d094531d273";
const ETH_BRIDGE = "56dbd72e1b8e77304d5d9c2cc01f0b6df5e7ccf2fe528c51a856445c4b749e4d";
const DEPOSIT_AMOUNT = "0.1";
const ROUTING = "bc1qw5sz2b7dl8mkvcz5kw83dzbn3xhg8c7klqkkzh";
const TOKEN_PATH = [
    { symbol: "", asset: "ETH" as string, price: "0.0625", amount: "0.05", slippage: "0.05", buying: true },
    { symbol: "USDT", asset: ["ETH", "USDT", "0xdAC17F958D2ee523a2206206994597C13D831ec7"] as [string, string, string], price: "0.001", amount: "0.05", slippage: "0.05", buying: true }
];
const FEE_PATH = [
    { symbol: "", asset: "ETH" as string, price: "1000", amount: "0.000960568", slippage: "0.05", buying: true }
];
const RESPEND_AMOUNT = "1561.404130219803563975";
const RESPEND_ASSET: [string, string, string] = ["ETH", "USDT", "0xdAC17F958D2ee523a2206206994597C13D831ec7"];
const RESPEND_BRIDGE = ETH_BRIDGE;
const entropy = new Uint8Array(32).fill(0xAB);

interface Swap {
    symbol: string;
    asset: string | [string, string, string];
    price: string;
    amount: string;
    slippage: string;
    buying: boolean;
}

function hash256(data: Uint8Array): Uint256 {
    return new Uint256(blake2b(data, { dkLen: 32 }));
}
function assetOf(asset: string | [string, string, string]): AssetId {
    return Array.isArray(asset) ? AssetId.fromHandle(asset[0], asset[1], asset[2]) : AssetId.fromHandle(asset);
}
function dec(hex: string): string {
    return new Uint256(hex, 16).toString();
}
function spendDeposit(depositHash: string, bridgeHash: string, asset: string, amount: string, toAddress: string, fromAddress: string, routing: string, path: Swap[], feePath: Swap[]): Uint8Array {
    const message = new Stream();
    message.writeInteger(new Uint256(depositHash, 16));
    message.writeInteger(new Uint256(bridgeHash, 16));
    message.writeInteger(assetOf(asset).toUint256());
    message.writeDecimal(new BigNumber(amount), true);
    message.writeBinaryStringOptimized((Signing.decodeAddress(toAddress) as Pubkeyhash).data);
    message.writeBinaryStringOptimized((Signing.decodeAddress(fromAddress) as Pubkeyhash).data);
    message.writeString(routing);
    message.writeInteger(path.length + feePath.length);
    for (const order of path.concat(feePath))
    {
        message.writeString(order.symbol);
        message.writeInteger(assetOf(order.asset).toUint256());
        message.writeDecimal(new BigNumber(order.price), true);
        message.writeDecimal(new BigNumber(order.amount), true);
        message.writeDecimal(new BigNumber(order.slippage), true);
        message.writeBoolean(order.buying);
    }
    return message.data;
}
function respendDeposit(depositHash: string, broadcastHash: string, bridgeHash: string, asset: string | [string, string, string], amount: string, toAddress: string, fromAddress: string, routing: string): Uint8Array {
    const message = new Stream();
    message.writeInteger(new Uint256(depositHash, 16));
    message.writeInteger(new Uint256(broadcastHash, 16));
    message.writeInteger(new Uint256(bridgeHash, 16));
    message.writeInteger(assetOf(asset).toUint256());
    message.writeDecimal(new BigNumber(amount));
    message.writeBinaryStringOptimized((Signing.decodeAddress(toAddress) as Pubkeyhash).data);
    message.writeBinaryStringOptimized((Signing.decodeAddress(fromAddress) as Pubkeyhash).data);
    message.writeString(routing);
    return message.data;
}
function signBlob(message: Uint8Array, secretKey: Seckey, publicKey: Pubkey): string {
    const signature = Signing.sign(hash256(message), secretKey);
    if (!signature)
        throw new Error("signing failed");

    const recovered = Signing.recover(hash256(message), signature);
    if (!recovered || !recovered.equals(publicKey))
        throw new Error("recovery mismatch");

    return ByteUtil.uint8ArrayToHexString(signature.data);
}

function main(): void {
    Chain.props = Chain.regtest;

    const T = "  ";
    const secretKey: Seckey = Signing.deriveSecretKey(entropy);
    const publicKey = Signing.derivePublicKey(secretKey);
    const spendMessage = spendDeposit(BTC_DEPOSIT, ETH_BRIDGE, "BTC", DEPOSIT_AMOUNT, AMS, SPENDER, ROUTING, TOKEN_PATH, FEE_PATH);
    const noRoutingMessage = spendDeposit(BTC_DEPOSIT, ETH_BRIDGE, "BTC", DEPOSIT_AMOUNT, AMS, SPENDER, "", TOKEN_PATH, FEE_PATH);
    const resentMessage = respendDeposit(BTC_DEPOSIT, BROADCAST, RESPEND_BRIDGE, RESPEND_ASSET, RESPEND_AMOUNT, AMS, SPENDER, ROUTING);
    const spendSig = signBlob(spendMessage, secretKey, publicKey);
    const noRoutingSig = signBlob(noRoutingMessage, secretKey, publicKey);
    const respendSig = signBlob(resentMessage, secretKey, publicKey);
    const swapRow = (order: Swap): string => {
        return `[${[JSON.stringify(order.symbol), Array.isArray(order.asset) ? `{ "$asset": [${order.asset.map(x => JSON.stringify(x)).join(", ")}] }` : JSON.stringify(assetOf(order.asset).toUint256().toString()), JSON.stringify(order.price), JSON.stringify(order.amount), JSON.stringify(order.slippage), order.buying ? "true" : "false"].join(", ")}]`;
    }
    const swapArray = (path: Swap[]): string => {
        return [
            `${T}${T}[`,
            ...path.map((o, i) => `${T}${T}${T}${swapRow(o)}${i < path.length - 1 ? "," : ""}`),
            `${T}${T}]`
        ].join("\n");
    }
    const spendStep = (kind: "try" | "", label: string, expectation: string, routing: string, sig: string): string => {
        return [
            `▶ STEP ${label} — replaces the ${kind ? '"try", "call"' : '"call"'} spend_deposit step with this expectation → ${expectation}`,
            `${T}[${kind === "try" ? "\"try\", " : ""}"call", "spend_deposit",`,
            `${T}${T}${dec(BTC_DEPOSIT)},`,
            `${T}${T}${dec(ETH_BRIDGE)},`,
            `${T}${T}${JSON.stringify(routing)},`,
            swapArray(TOKEN_PATH) + ",",
            swapArray(FEE_PATH) + ",",
            `${T}${T}{ "$blob": ${JSON.stringify(sig)} }`,
            kind === "try" ? `${T}],` : `${T}]`,
            ""
        ].join("\n");
    }
    const respendStep = (sig: string): string => {
        return [
            `▶ STEP respend_deposit — replaces the ["call", "respend_deposit", ...] step; requires RESPEND_AMOUNT == output.amount of the spent_booking event`,
            `${T}["call", "respend_deposit",`,
            `${T}${T}${dec(BTC_DEPOSIT)},`,
            `${T}${T}${dec(BROADCAST)},`,
            `${T}${T}${dec(RESPEND_BRIDGE)},`,
            `${T}${T}${JSON.stringify(ROUTING)},`,
            `${T}${T}{ "$blob": ${JSON.stringify(sig)} }`,
            `${T}]`,
            ""
        ].join("\n");
    }
    const bookStep = (publicKey: Pubkey): string => {
        return [
            `▶ STEP book_deposit — replaces the ["call", "book_deposit", ...] step`,
            `${T}["call", "book_deposit", { "$asset": ["BTC"] }, ${JSON.stringify(DEPOSIT_AMOUNT)}, { "$blob": ${JSON.stringify(ByteUtil.uint8ArrayToHexString(publicKey.data))} }]`,
            ""
        ].join("\n");
    }
    const banner = (text: string): string => {
        return "═══════════════════════════════════════════════════════════════════════════\n" + text + "\n═══════════════════════════════════════════════════════════════════════════";
    }
    const valuesOnly = (spendMessage: Uint8Array, resentMessage: Uint8Array, spendSig: string, noRoutingSig: string, respendSig: string, publicKey: Pubkey): string => {
        return JSON.stringify({
            book_deposit_public_key: ByteUtil.uint8ArrayToHexString(publicKey.data),
            spend_signature: spendSig,
            respend_signature: respendSig,
            spend_message_hash: hash256(spendMessage).toHex(),
            respend_message_hash: hash256(resentMessage).toHex(),
            btc_deposit_hash: dec(BTC_DEPOSIT),
            eth_deposit_hash: new Uint256(ETH_DEPOSIT).toString(),
            broadcast_hash: dec(BROADCAST),
            btc_bridge_hash: dec(BTC_BRIDGE),
            eth_bridge_hash: dec(ETH_BRIDGE),
            btc_asset_id: assetOf("BTC").toUint256().toString(),
            eth_asset_id: assetOf("ETH").toUint256().toString(),
            eth_usdt_asset_id: assetOf(["ETH", "USDT", "0xdAC17F958D2ee523a2206206994597C13D831ec7"]).toUint256().toString(),
            unified_usdt_asset_id: AssetId.fromHandle("TAN", "USDT", DEX).toUint256().toString()
        }, null, 2);
    }
    console.log(bookStep(publicKey));
    console.log(spendStep("try", "try #1 (flags off, routing set)", "must fail: send not permitted", ROUTING, spendSig));
    console.log(spendStep("try", "try #2 (flags off, empty routing)", "must fail: send not permitted", "", noRoutingSig));
    console.log(spendStep("try", "try #3 (flags withdraw_with_revert, empty sig)", "must fail: booking signature does not match", ROUTING, "0x00"));
    console.log(spendStep("", "FINAL spend — the verified one", "must succeed, stages reverted withdraw broadcast", ROUTING, spendSig));
    console.log(respendStep(respendSig));
    console.log(banner(" VALUES ONLY — for surgical edits (grep-friendly)"));
    console.log(valuesOnly(spendMessage, resentMessage, spendSig, noRoutingSig, respendSig, publicKey));
}

main();