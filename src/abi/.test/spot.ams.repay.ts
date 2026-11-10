// npx tsc src/abi/.test/spot.ams.repay.ts --module commonjs --target es2020 --esModuleInterop --skipLibCheck --strict false
// node src/abi/.test/spot.ams.repay.js <ams-contract-address> <deposit-tx-hash> <secret-key> [broadcast-tx-hash] [options]

import { AssetId, Chain, Hashsig, Messages, RPC, SchemaUtil, Seckey, Signing, Stream, Transactions, UiUtil, Uint256 } from "tangentsdk";
import BigNumber from "bignumber.js";

const SIMULATION_GAS = new Uint256(1_000_000);

type ParsedArgs = { positionals: string[], flags: Record<string, string | boolean> };

function parseArgs(argv: string[]): ParsedArgs {
    const positionals: string[] = [];
    const flags: Record<string, string | boolean> = {};
    for (let i = 0; i < argv.length; i++) {
        const item = argv[i];
        if (!item.startsWith("--")) {
            positionals.push(item);
            continue;
        }

        const name = item.slice(2);
        const next = argv[i + 1];
        flags[name] = next != null && !next.startsWith("--") ? (i++, next) : true;
    }

    return { positionals, flags };
}
function secretKey(value: string): Seckey {
    const decoded = Signing.decodeSecretKey(value);
    if (decoded != null)
        return decoded;

    const hex = (value.startsWith("0x") || value.startsWith("0X")) ? value.slice(2) : value;
    if (/^[0-9a-fA-F]{64}$/.test(hex))
        return new Seckey(hex);

    throw new Error("invalid secret key; expected bech32 ('sec…' / 'secrt1…') or 64 hex characters");
}
function hashOf(value: string): Uint256 {
    const raw = value.trim();
    const hex = (raw.startsWith("0x") || raw.startsWith("0X")) ? raw.slice(2) : raw;
    if (/^[0-9a-fA-F]{64}$/.test(hex))
        return new Uint256(hex, 16);

    if (/^\d+$/.test(raw))
        return new Uint256(raw);

    throw new Error("invalid transaction hash; expected a 32-byte hex value or a decimal uint256");
}
function buildRepay(ams: string, deposit: Uint256, broadcast: Uint256, seckey: Seckey, nonce: Uint256, gasPrice: BigNumber, gasLimit: Uint256): Stream {
    const callable = Signing.decodeAddress(ams);
    if (callable == null)
        throw new Error(`AMS contract address is not valid for the selected network: ${ams}`);

    const transaction = {
        signature: new Hashsig(),
        asset: new AssetId(),
        nonce: nonce,
        gasPrice: gasPrice,
        gasLimit: gasLimit,
        callable: callable,
        pays: [],
        function: UiUtil.toFunction("repay_deposit"),
        args: [deposit, broadcast]
    };

    let signing = new Stream();
    SchemaUtil.store(signing, transaction, Messages.asSigningSchema(new Transactions.Call()));
    const signature = Signing.sign(signing.hash(), seckey);
    if (!signature)
        throw new Error("failed to sign the transaction");

    const recovered = Signing.recover(signing.hash(), signature);
    if (!recovered || !recovered.equals(Signing.derivePublicKey(seckey)))
        throw new Error("signature recovery mismatch; the secret key does not match its public key");

    const stream = new Stream();
    SchemaUtil.store(stream, { ...transaction, signature: signature }, new Transactions.Call());
    return stream;
}

async function main(): Promise<void> {
    const { positionals, flags } = parseArgs(process.argv.slice(2));
    if (positionals.length < 3) {
        console.error("usage: node spot.ams.repay.js <ams-contract-address> <deposit-tx-hash> <secret-key> [broadcast-tx-hash] [--node host:port] [--gas-price n] [--gas-limit n] [--mainnet] [--send]");
        process.exit(1);
    }

    const ams = positionals[0];
    const deposit = hashOf(positionals[1]);
    const seckey = secretKey(positionals[2]);
    const broadcast = positionals.length > 3 ? hashOf(positionals[3]) : new Uint256(0);
    Chain.props = flags["mainnet"] === true ? Chain.mainnet : Chain.regtest;

    const deployer = Signing.encodeAddress(Signing.derivePublicKeyHash(Signing.derivePublicKey(seckey)));
    RPC.applyValidator(typeof flags["node"] == "string" ? flags["node"] : "localhost:18419");

    const next = await RPC.getNextAccountNonce(deployer);
    if (next == null)
        throw new Error(`failed to fetch the next account nonce for ${deployer} from the node (is --node reachable?)`);
    const nonce = new Uint256(next.toString());
    const gasPrice = flags["gas-price"] != null ? new BigNumber(String(flags["gas-price"])) : new BigNumber((await RPC.getGasPrice(new AssetId(), 0.95))?.price?.toString() || 0);

    let gasLimit = flags["gas-limit"] != null ? new Uint256(String(flags["gas-limit"])) : null;
    let simulation: { successful: boolean, message: string } | null = null;
    if (gasLimit == null) {
        const probe = buildRepay(ams, deposit, broadcast, seckey, nonce, gasPrice, SIMULATION_GAS);
        const receipt = await RPC.simulateTransaction(probe.encode());
        const used = new Uint256(receipt?.relative_gas_use?.toString() || 0);
        simulation = { successful: receipt?.successful === true, message: receipt?.message ?? receipt?.error ?? "no simulation receipt" };
        gasLimit = simulation.successful && used.gt(0) ? used : SIMULATION_GAS;
    }

    const transaction = buildRepay(ams, deposit, broadcast, seckey, nonce, gasPrice, gasLimit);
    const encoded = transaction.encode();

    console.log(JSON.stringify({
        deployer,
        contract: ams,
        deposit_hash: deposit.toString(),
        broadcast_hash: broadcast.toString(),
        nonce: nonce.toString(),
        gas_price: gasPrice.toString(),
        gas_limit: gasLimit.toString(),
        simulation,
        transaction: encoded
    }, null, 2));

    if (flags["send"] === true) {
        if (simulation != null && !simulation.successful)
            throw new Error(`refusing to submit: simulation rejected the transaction (${simulation.message})`);
        const transactionHash = await RPC.submitTransaction(encoded);
        console.log("\nsubmitted:", transactionHash);
    } else
        console.log("\nsigned transaction ready; re-run with --send to broadcast, or submit the 'transaction' hex via submittransaction");

    await RPC.disconnectSocket();
}

main().catch((exception) => {
    console.error("error:", exception?.message ?? exception);
    process.exit(1);
});
