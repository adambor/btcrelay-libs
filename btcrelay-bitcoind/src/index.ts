import * as bitcoin from "bitcoinjs-lib";
import * as tinySecpk256Interface from "@bitcoinerlab/secp256k1";
bitcoin.initEccLib(tinySecpk256Interface);

export * from "./rpc/BitcoindBlock";
export * from "./rpc/BitcoindRpc";
export * from "./rpc/BTCMerkleTree";

export * from "./synchronizer/BitcoindRelaySynchronizer";
