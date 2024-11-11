import {PrunedTxoMap} from "./PrunedTxoMap";
import * as fs from "fs/promises";
import {SavedSwap} from "./SavedSwap";
import {
    BtcStoredHeader,
    InitializeEvent,
    SwapEvent,
    ChainSwapType,
    BitcoinRpc,
    SwapDataVerificationError,
    ChainType
} from "crosslightning-base";


export class Watchtower<T extends ChainType, B extends BtcStoredHeader<any>> {

    readonly hashMap: Map<string, SavedSwap<T>> = new Map<string, SavedSwap<T>>();
    readonly escrowMap: Map<string, SavedSwap<T>> = new Map<string, SavedSwap<T>>();

    readonly btcRelay: T["BtcRelay"];

    readonly swapContract: T["Contract"];
    readonly solEvents: T["Events"];
    readonly signer: T["Signer"];

    readonly bitcoinRpc: BitcoinRpc<any>;

    readonly prunedTxoMap: PrunedTxoMap;

    readonly dirName: string;
    readonly rootDir: string;

    readonly shouldClaimCbk: (swap: SavedSwap<T>) => Promise<{initAta: boolean, feeRate: any}>;

    constructor(
        directory: string,
        btcRelay: T["BtcRelay"],
        solEvents: T["Events"],
        swapContract: T["Contract"],
        signer: T["Signer"],
        bitcoinRpc: BitcoinRpc<any>,
        pruningFactor?: number,
        shouldClaimCbk?: (swap: SavedSwap<T>) => Promise<{initAta: boolean, feeRate: any}>
    ) {
        this.rootDir = directory;
        this.dirName = directory+"/swaps";
        this.btcRelay = btcRelay;
        this.solEvents = solEvents;
        this.swapContract = swapContract;
        this.signer = signer;
        this.bitcoinRpc = bitcoinRpc;
        this.prunedTxoMap = new PrunedTxoMap(directory+"/wt-height.txt", bitcoinRpc, pruningFactor);
        this.shouldClaimCbk = shouldClaimCbk;
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
        },
        initAta?: boolean,
        feeRate?: any
    ): Promise<T["TX"][] | null> {
        const isCommited = await this.swapContract.isCommited(swap.swapData);

        if(!isCommited) {
            console.log("[Watchtower.createClaimTxs]: Not claiming swap txoHash: "+txoHash.toString("hex")+" due to it not being commited anymore!");
            return null;
        }

        console.log("[Watchtower.createClaimTxs]: Claim swap txns: "+swap.hash.toString("hex")+" UTXO: ", txId+":"+voutN+"@"+blockheight);

        const tx = await this.bitcoinRpc.getTransaction(txId);

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
            txs = await this.swapContract.txsClaimWithTxData(this.signer, swap.swapData, blockheight, tx, voutN, storedHeader, null, initAta==null ? false : initAta, feeRate);
        } catch (e) {
            if(e instanceof SwapDataVerificationError) {
                console.log("[Watchtower.createClaimTxs] Not claiming swap txoHash: "+txoHash.toString("hex")+" due to SwapDataVerificationError!");
                console.error(e);
                return null;
            }
            throw e;
        }
        return txs;

    }

    private async claim(txoHash: Buffer, swap: SavedSwap<T>, txId: string, vout: number, blockheight: number): Promise<boolean> {

        console.log("[Watchtower.claim]: Claim swap: "+swap.hash.toString("hex")+" UTXO: ", txId+":"+vout+"@"+blockheight);

        try {
            const unlock = swap.lock(120);

            if(unlock==null) return false;

            let feeData;
            if(this.shouldClaimCbk!=null) {
                feeData = await this.shouldClaimCbk(swap);
                if(feeData==null) {
                    console.log("[Watchtower.claim] Not claiming swap with txoHash: "+txoHash.toString("hex")+" due to negative response from shouldClaimCbk() callback!");
                    return false;
                }
                console.log("[Watchtower.claim] Claiming swap with txoHash: "+txoHash+" initAta: "+feeData.initAta+" feeRate: "+feeData.feeRate);
            } else {
                console.log("[Watchtower.claim] Claiming swap with txoHash: "+txoHash);
            }

            try {
                await this.swapContract.claimWithTxData(this.signer, swap.swapData, blockheight, await this.bitcoinRpc.getTransaction(txId), vout, null, null, feeData?.initAta==null ? false : feeData.initAta, {
                    waitForConfirmation: true,
                    feeRate: feeData?.feeRate
                });
            } catch (e) {
                if(e instanceof SwapDataVerificationError) {
                    await this.remove(swap.txoHash);
                    return false;
                }
                return false;
            }

            console.log("[Watchtower.claim]: Claim swap: "+swap.hash.toString("hex")+" success!");

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

        console.log("[Watchtower.init]: Loaded!");

        this.solEvents.registerListener(async (obj: SwapEvent<T["Data"]>[]) => {
            for(let event of obj) {
                if(event instanceof InitializeEvent) {
                    if(event.swapType!==ChainSwapType.CHAIN) {
                        continue;
                    }

                    const swapData = await event.swapData();

                    const txoHash: Buffer = Buffer.from(swapData.getTxoHash(), "hex");
                    const hash: Buffer = Buffer.from(swapData.getHash(), "hex");

                    if(txoHash.equals(Buffer.alloc(32, 0))) continue; //Opt-out flag

                    const txoHashHex = txoHash.toString("hex");

                    //Check with pruned tx map
                    const data = this.prunedTxoMap.getTxoObject(txoHashHex);

                    const savedSwap: SavedSwap<T> = new SavedSwap<T>(txoHash, hash, swapData.getConfirmations(), swapData);

                    console.log("[Watchtower.chainEvents]: Adding new swap to watchlist: ", savedSwap);

                    await this.save(savedSwap);
                    if(data!=null) {
                        const requiredBlockHeight = data.height+savedSwap.confirmations-1;
                        if(requiredBlockHeight<=this.prunedTxoMap.tipHeight) {
                            //Claimable
                            const isCommited = await this.swapContract.isCommited(swapData);
                            if(isCommited) {
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

        console.log("[Watchtower.init]: Synchronized sol events");

        const resp = await this.btcRelay.retrieveLatestKnownBlockLog();

        //Sync to previously processed block
        await this.prunedTxoMap.init(resp.resultBitcoinHeader.height);

        for(let txoHash of this.escrowMap.keys()) {
            const data = this.prunedTxoMap.getTxoObject(txoHash);
            console.log("[Watchtower.init] Check "+txoHash+":", data);
            if(data!=null) {
                const savedSwap = this.escrowMap.get(txoHash);
                const requiredBlockHeight = data.height+savedSwap.confirmations-1;
                if(requiredBlockHeight<=resp.resultBitcoinHeader.height) {
                    //Claimable
                    await this.claim(Buffer.from(txoHash, "hex"), savedSwap, data.txId, data.vout, data.height);
                }
            }
        }

        console.log("[Watchtower.init]: Synced to last processed block");

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

        console.log("[Watchtower.init]: Synced to last btc relay block");
    }

    async syncToTipHash(
        tipBlockHash: string,
        computedHeaderMap?: {[blockheight: number]: B}
    ): Promise<{
        [txcHash: string]: {
            txs: T["TX"][],
            txId: string,
            vout: number,
            maturedAt: number,
            hash: Buffer
        }
    }> {
        console.log("[Watchtower.syncToTipHash]: Syncing to tip hash: ", tipBlockHash);

        const txs: {
            [txcHash: string]: {
                txs: T["TX"][],
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

                    //Check claimer's bounty and create ATA if the claimer bounty covers the costs of it!

                    let claimTxs: T["TX"][];
                    if(this.shouldClaimCbk!=null) {
                        const feeData = await this.shouldClaimCbk(savedSwap);
                        if(feeData==null) {
                            console.log("[Watchtower.syncToTipHash] Not claiming swap with txoHash: "+txoHash+" due to negative response from shouldClaimCbk() callback!");
                            continue;
                        }
                        console.log("[Watchtower.syncToTipHash] Claiming swap with txoHash: "+txoHash+" initAta: "+feeData.initAta+" feeRate: "+feeData.feeRate);
                        claimTxs = await this.createClaimTxs(Buffer.from(txoHash, "hex"), savedSwap, data.txId, data.vout, data.height, computedHeaderMap, feeData.initAta, feeData.feeRate);
                    } else {
                        console.log("[Watchtower.syncToTipHash] Claiming swap with txoHash: "+txoHash);
                        claimTxs = await this.createClaimTxs(Buffer.from(txoHash, "hex"), savedSwap, data.txId, data.vout, data.height, computedHeaderMap);
                    }

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

                        //Check claimer's bounty and create ATA if the claimer bounty covers the costs of it!

                        let claimTxs: T["TX"][];
                        if(this.shouldClaimCbk!=null) {
                            const feeData = await this.shouldClaimCbk(savedSwap);
                            if(feeData==null) {
                                console.log("[Watchtower.syncToTipHash] Not claiming swap with txoHash: "+txoHash+" due to negative response from shouldClaimCbk() callback!");
                                continue;
                            }
                            console.log("[Watchtower.syncToTipHash] Claiming swap with txoHash: "+txoHash+" initAta: "+feeData.initAta+" feeRate: "+feeData.feeRate);
                            claimTxs = await this.createClaimTxs(Buffer.from(txoHash, "hex"), savedSwap, data.txId, data.vout, data.height, computedHeaderMap, feeData.initAta, feeData.feeRate);
                        } else {
                            console.log("[Watchtower.syncToTipHash] Claiming swap with txoHash: "+txoHash);
                            claimTxs = await this.createClaimTxs(Buffer.from(txoHash, "hex"), savedSwap, data.txId, data.vout, data.height, computedHeaderMap);
                        }

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
