const sqlite3 = require('sqlite3').verbose();

class TableReducer {
    db = null;
    SOURCE_TABLE_NAME = 'oa_trf_src'
    REDUCED_TABLE_NAME = 'oa_trf_src_red'
    colsToReduce = []
    allCols = []
    unmodifiedCols = []

    constructor(dbName){
        this.db = new sqlite3.Database(dbName, (err)=>{
            if(err) return console.error(`Could not connect to the db ${err.message}`)
        });

    }

    async createMapperTable(){
        return await new Promise((resolve, reject) => {
            
        })
    }

    fillMapperTable(){

    }

    createReducerTable(){

    }

    fillReducerTable(){

    }

    run(){
        if(!this.db) return;

        this.db.close()
    }
}

module.exports = TableReducer;