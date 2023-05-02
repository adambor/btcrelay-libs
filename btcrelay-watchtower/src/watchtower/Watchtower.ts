import {PrunedTxoMap} from "./PrunedTxoMap";
import * as fs from "fs/promises";
import {SavedSwap} from "./SavedSwap";
import {BtcStoredHeader, InitializeEvent, SwapEvent, ChainEvents, SwapContract, SwapData, ChainSwapType, BitcoinRpc, BtcRelay,
    SwapDataVerificationError, BtcBlock, RelaySynchronizer} from "crosslightning-base";


export class Watchtower<T extends SwapData, B extends BtcStoredHeader<any>, TX> {

    hashMap: Map<string, SavedSwap<T>> = new Map<string, SavedSwap<T>>();
    escrowMap: Map<string, SavedSwap<T>> = new Map<string, SavedSwap<T>>();

    btcRelay: BtcRelay<B, TX, any>;
    btcRelaySynchronizer: RelaySynchronizer<B, TX, BtcBlock>;

    swapContract: SwapContract<T, TX>;
    solEvents: ChainEvents<T>;
    bitcoinRpc: BitcoinRpc<any>;

    prunedTxoMap: PrunedTxoMap;

    readonly dirName;
    readonly rootDir;

    constructor(
        directory: string,
        btcRelay: BtcRelay<B, TX, any>,
        btcRelaySynchronizer: RelaySynchronizer<B, TX, BtcBlock>,
        solEvents: ChainEvents<T>,
        swapContract: SwapContract<T, TX>,
        bitcoinRpc: BitcoinRpc<any>,
        pruningFactor?: number
    ) {
        this.rootDir = directory;
        this.dirName = directory+"/swaps";
        this.btcRelay = btcRelay;
        this.btcRelaySynchronizer = btcRelaySynchronizer;
        this.solEvents = solEvents;
        this.swapContract = swapContract;
        this.bitcoinRpc = bitcoinRpc;
        this.prunedTxoMap = new PrunedTxoMap(directory+"/wt-height.txt", bitcoinRpc, pruningFactor);
    }

    private async load() {
        try {
            await fs.mkdir(this.dirName);
        } catch (e) {}

        let files;
        try {
            files = await fs.readdir(this.dirName);
        } catch (e) {
            console.error(e);
        }

        if(files==null) return;

        for(let file of files) {
            const txoHashHex = file.split(".")[0];
            const result = await fs.readFile(this.dirName+"/"+file);
            const escrowData = JSON.parse(result.toString());
            escrowData.txoHash = txoHashHex;

            const savedSwap = new SavedSwap<T>(escrowData);

            this.escrowMap.set(txoHashHex, savedSwap);
            this.hashMap.set(savedSwap.hash.toString("hex"), savedSwap);
        }
    }

    private async save(swap: SavedSwap<T>) {
        try {
            await fs.mkdir(this.dirName)
        } catch (e) {}

        const cpy = swap.serialize();

        this.escrowMap.set(swap.txoHash.toString("hex"), swap);
        this.hashMap.set(swap.hash.toString("hex"), swap);

        await fs.writeFile(this.dirName+"/"+swap.txoHash.toString("hex")+".json", JSON.stringify(cpy));
    }

    private async remove(txoHash: Buffer): Promise<boolean> {
        const retrieved = this.escrowMap.get(txoHash.toString("hex"));
        if(retrieved==null) return false;

        const txoHashHex = txoHash.toString("hex");
        try {
            await fs.rm(this.dirName+"/"+txoHashHex+".json");
        } catch (e) {
            console.error(e);
        }

        this.escrowMap.delete(txoHash.toString("hex"));
        this.hashMap.delete(retrieved.hash.toString("hex"));

        return true;
    }

    private async removeByHash(hash: Buffer): Promise<boolean> {
        const retrieved = this.hashMap.get(hash.toString("hex"));
        if(retrieved==null) return false;

        const txoHashHex = retrieved.txoHash.toString("hex");
        try {
            await fs.rm(this.dirName+"/"+txoHashHex+".json");
        } catch (e) {
            console.error(e);
        }

        this.escrowMap.delete(retrieved.txoHash.toString("hex"));
        this.hashMap.delete(hash.toString("hex"));

        return true;
    }

    private async createClaimTxs(
        txoHash: Buffer,
        swap: SavedSwap<T>,
        txId: string,
        voutN: number,
        blockheight: number,
        computedCommitedHeaders?: {
            [height: number]: B
        }
    ): Promise<TX[] | null> {
        const isCommited = await this.swapContract.isCommited(swap.swapData);

        if(!isCommited) return null;

        const tx = await this.bitcoinRpc.getTransaction(txId);
        // const tx = await new Promise<BitcoindTransaction>((resolve, reject) => {
        //     BtcRPC.getRawTransaction(txId, 1, (err, info) => {
        //         if(err) {
        //             reject(err);
        //             return;
        //         }
        //         resolve(info.result);
        //     });
        // });
        //
        // //Strip witness data
        // const btcTx = bitcoin.Transaction.fromHex(tx.hex);
        // for(let txIn of btcTx.ins) {
        //     txIn.witness = [];
        // }
        // tx.hex = btcTx.toHex();

        //Re-check txoHash
        const vout = tx.outs[voutN];
        const computedTxoHash = PrunedTxoMap.toTxoHash(vout.value, vout.scriptPubKey.hex);

        if(!txoHash.equals(computedTxoHash)) throw new Error("TXO hash mismatch");

        if(tx.confirmations<swap.swapData.getConfirmations()) throw new Error("Not enough confirmations yet");

        let storedHeader: B = null;
        if(computedCommitedHeaders!=null) {
            storedHeader = computedCommitedHeaders[blockheight];
        }

        let txs;
        try {
            txs = await this.swapContract.txsClaimWithTxData(swap.swapData, blockheight, tx, voutN, storedHeader, null, false);
        } catch (e) {
            if(e instanceof SwapDataVerificationError) return null;
            throw e;
        }
        return txs;

    }

    private async claim(txoHash: Buffer, swap: SavedSwap<T>, txId: string, vout: number, blockheight: number): Promise<boolean> {

        console.log("[Watchtower]: Claim swap: "+swap.hash.toString("hex")+" UTXO: ", txId+":"+vout+"@"+blockheight);

        try {
            const unlock = swap.lock(120);

            if(unlock==null) return false;

            try {
                await this.swapContract.claimWithTxData(swap.swapData, blockheight, await this.bitcoinRpc.getTransaction(txId), vout, null, null, false, true);
            } catch (e) {
                if(e instanceof SwapDataVerificationError) {
                    await this.remove(swap.txoHash);
                    return false;
                }
                return false;
            }

            console.log("[Watchtower]: Claim swap: "+swap.hash.toString("hex")+" success!");

            await this.remove(txoHash);

            unlock();

            return true;
        } catch (e) {
            console.error(e);
            return false;
        }

    }

    async init() {
        try {
            await fs.mkdir(this.rootDir);
        } catch (e) {}

        await this.load();

        console.log("[Watchtower]: Loaded!");

        this.solEvents.registerListener(async (obj: SwapEvent<T>[]) => {
            for(let event of obj) {
                if(event instanceof InitializeEvent) {
                    const swapData = event.swapData;

                    if(swapData.getType()!==ChainSwapType.CHAIN) {
                        continue;
                    }

                    const txoHash: Buffer = Buffer.from(swapData.getTxoHash(), "hex");
                    const hash: Buffer = Buffer.from(swapData.getHash(), "hex");

                    if(txoHash.equals(Buffer.alloc(32, 0))) continue; //Opt-out flag

                    const txoHashHex = txoHash.toString("hex");

                    //Check with pruned tx map
                    const data = this.prunedTxoMap.getTxoObject(txoHashHex);

                    const isCommited = await this.swapContract.isCommited(swapData);
                    if(isCommited) {
                        const savedSwap: SavedSwap<T> = new SavedSwap<T>(txoHash, hash, swapData.getConfirmations(), event.swapData);

                        console.log("[Watchtower]: Adding new swap to watchlist: ", savedSwap);

                        await this.save(savedSwap);
                        if(data!=null) {
                            const requiredBlockHeight = data.height+savedSwap.confirmations-1;
                            if(requiredBlockHeight<=this.prunedTxoMap.tipHeight) {
                                //Claimable
                                await this.claim(txoHash, savedSwap, data.txId, data.vout, data.height);
                            }
                        }
                    }
                } else {
                    const hash: Buffer = Buffer.from(event.paymentHash, "hex");
                    const success = await this.removeByHash(hash);
                    if(success) {
                        console.log("[Watchtower]: Removed swap from watchlist: ", hash.toString("hex"));
                    }
                }
            }
            return true;
        });

        //Sync to latest on Solana
        await this.solEvents.init();

        console.log("[Watchtower]: Synchronized sol events");

        const resp = await this.btcRelay.retrieveLatestKnownBlockLog();

        //Sync to previously processed block
        await this.prunedTxoMap.init(resp.resultBitcoinHeader.height);

        for(let txoHash of this.escrowMap.keys()) {
            const data = this.prunedTxoMap.getTxoObject(txoHash);
            console.log("[Watchtower] Check "+txoHash+":", data);
            if(data!=null) {
                const savedSwap = this.escrowMap.get(txoHash);
                const requiredBlockHeight = data.height+savedSwap.confirmations-1;
                if(requiredBlockHeight<=resp.resultBitcoinHeader.height) {
                    //Claimable
                    await this.claim(Buffer.from(txoHash, "hex"), savedSwap, data.txId, data.vout, data.height);
                }
            }
        }

        console.log("[Watchtower]: Synced to last processed block");

        //Sync to the btc relay height
        const includedTxoHashes = await this.prunedTxoMap.syncToTipHash(resp.resultBitcoinHeader.hash, this.escrowMap);

        //Check if some of the txoHashes got confirmed
        for(let entry of includedTxoHashes.entries()) {
            const txoHash = entry[0];
            const data = entry[1];

            const savedSwap = this.escrowMap.get(txoHash);
            const requiredBlockHeight = data.height+savedSwap.confirmations-1;
            if(requiredBlockHeight<=resp.resultBitcoinHeader.height) {
                //Claimable
                await this.claim(Buffer.from(txoHash, "hex"), savedSwap, data.txId, data.vout, data.height);
            }
        }

        console.log("[Watchtower]: Synced to last btc relay block");
    }

    async syncToTipHash(
        tipBlockHash: string,
        computedHeaderMap?: {[blockheight: number]: B}
    ): Promise<{
        [txcHash: string]: {
            txs: TX[],
            txId: string,
            vout: number,
            maturedAt: number,
            hash: Buffer
        }
    }> {
        console.log("[Watchtower]: Syncing to tip hash: ", tipBlockHash);

        const txs: {
            [txcHash: string]: {
                txs: TX[],
                txId: string,
                vout: number,
                maturedAt: number,
                blockheight: number,
                hash: Buffer
            }
        } = {};

        //Check txoHashes that got required confirmations in these blocks,
        // but they might be already pruned if we only checked after
        const includedTxoHashes = await this.prunedTxoMap.syncToTipHash(tipBlockHash, this.escrowMap);

        for(let entry of includedTxoHashes.entries()) {
            const txoHash = entry[0];
            const data = entry[1];

            const savedSwap = this.escrowMap.get(txoHash);
            const requiredBlockHeight = data.height+savedSwap.confirmations-1;
            if(requiredBlockHeight<=this.prunedTxoMap.tipHeight) {
                //Claimable
                try {
                    const unlock = savedSwap.lock(120);
                    if(unlock==null) continue;

                    const claimTxs = await this.createClaimTxs(Buffer.from(txoHash, "hex"), savedSwap, data.txId, data.vout, data.height, computedHeaderMap);
                    if(claimTxs==null)  {
                        await this.remove(savedSwap.txoHash);
                    } else {
                        txs[txoHash] = {
                            txs: claimTxs,
                            txId: data.txId,
                            vout: data.vout,
                            blockheight: data.height,
                            maturedAt: data.height+savedSwap.confirmations-1,
                            hash: savedSwap.hash
                        }
                    }
                } catch (e) {
                    console.error(e);
                }
            }
        }

        //Check all the txs, if they are already confirmed in these blocks
        for(let txoHash of this.escrowMap.keys()) {
            const data = this.prunedTxoMap.getTxoObject(txoHash);
            if(data!=null) {
                const savedSwap = this.escrowMap.get(txoHash);
                const requiredBlockHeight = data.height+savedSwap.confirmations-1;
                if(requiredBlockHeight<=this.prunedTxoMap.tipHeight) {
                    //Claimable
                    try {
                        const unlock = savedSwap.lock(120);
                        if(unlock==null) continue;

                        const claimTxs = await this.createClaimTxs(Buffer.from(txoHash, "hex"), savedSwap, data.txId, data.vout, data.height, computedHeaderMap);
                        if(claimTxs==null) {
                            await this.remove(savedSwap.txoHash);
                        } else {
                            txs[txoHash] = {
                                txs: claimTxs,
                                txId: data.txId,
                                vout: data.vout,
                                blockheight: data.height,
                                maturedAt: data.height+savedSwap.confirmations-1,
                                hash: savedSwap.hash
                            }
                        }
                    } catch (e) {
                        console.error(e);
                    }
                }
            }
        }

        return txs;
    }

}
