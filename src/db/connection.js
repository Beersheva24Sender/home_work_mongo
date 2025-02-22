import { MongoClient, ServerApiVersion } from "mongodb";

export default class MongoConnection {
    #client;
    #db;

    constructor(connectionStr, dbName) {
        this.#client = new MongoClient(connectionStr, {
            serverApi: {
                version: ServerApiVersion.v1,
                strict: true,
                deprecationErrors: true,
            }
        });
        this.#db = this.#client.db(dbName);
    }

    getCollection(collectionName) {
        return this.#db.collection(collectionName);
    }

    get client() {
        return this.#client;
    }

    get db() {
        return this.#db;
    }

    async close() {
        await this.#client.close();
    }
}