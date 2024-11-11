import {Lockable} from "./Lockable";
import {ChainType, SwapData} from "crosslightning-base";

export class SavedSwap<T extends ChainType> extends Lockable {

    readonly txoHash: Buffer;
    readonly hash: Buffer;
    readonly confirmations: number;

    readonly swapData: T["Data"];

    constructor(data: any);
    constructor(txoHash: Buffer, hash: Buffer, confirmations: number, swapData: T["Data"]);

    constructor(txoHashOrObj: Buffer | any, hash?: Buffer, confirmations?: number, swapData?: T["Data"]) {
        super();
        if(hash!=null || confirmations!=null) {
            this.txoHash = txoHashOrObj;
            this.hash = hash;
            this.confirmations = confirmations;
            this.swapData = swapData;
        } else {
            this.txoHash = Buffer.from(txoHashOrObj.txoHash, "hex");
            this.hash = Buffer.from(txoHashOrObj.hash, "hex");
            this.confirmations = txoHashOrObj.confirmations;
            this.swapData = SwapData.deserialize(txoHashOrObj.swapData);
        }
    }

    serialize(): any {
        return {
            txoHash: this.txoHash.toString("hex"),
            hash: this.hash.toString("hex"),
            confirmations: this.confirmations,
            swapData: this.swapData.serialize()
        }
    }

}
