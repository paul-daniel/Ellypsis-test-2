const sqlite3 = require('sqlite3').verbose();

class TableReducer {
    db = null;
    SOURCE_TABLE_NAME = 'oa_trf_src'
    REDUCED_TABLE_NAME = 'oa_trf_src_red'
    colsToReduce = []
    allCols = []
    unmodifiedCols = ["impact"]

    constructor(dbName){
        this.db = new sqlite3.Database(dbName, (err)=>{
            if(err) return console.error(`Could not connect to the db ${err.message}`)
        });
    }

    getCols(){
        return new Promise((resolve, reject) => {
            this.db.all(`PRAGMA table_info(${this.SOURCE_TABLE_NAME})`, (err, rows) => {
                if(err) {
                    console.log("Error in PRAGMA: ", err);
                    return reject(err);
                }
                this.allCols = rows.map(row => row.name)
                
                this.colsToReduce = this.allCols.filter(col => !this.unmodifiedCols.includes(col))
                resolve()
            })
        }) 
    }

    createMapperTable(){
        return new Promise((resolve, reject) => {
            this.db.all(`PRAGMA table_info(${this.SOURCE_TABLE_NAME})`, (err, rows) => {
                if(err) {
                    console.log("Error in PRAGMA: ", err);
                    return reject(err);
                }

                const creationPromises = this.colsToReduce.map(col => {
                    const type = rows.find(row => row.name === col).type;
                    if(!type) {
                        console.log("Type not found for ", col);
                        return reject(`${col} does not exist`);
                    }
    
                    const tableName = `oa_trf_src_${col}_lkp`;
                    return new Promise((resolve, reject) => {
                        this.db.run(`CREATE TABLE IF NOT EXISTS ${tableName} (id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL, champ ${type})`, function(err) {
                            if (err) {
                                console.log("Error in table creation: ", err);
                                return reject(err);
                            }
                            resolve();
                        });
                    });
                });
    
                if(!creationPromises.length) {
                    console.log("Probably no columns exist");
                    return reject('Probably no columns exist');
                }
    
                Promise.all(creationPromises)
                .then(() => {
                    console.log('All mapper table created successfully');
                    resolve();
                })
                .catch(error => {
                    console.log(`Table creation failed: ${error}`);
                    reject(error);
                });
            });
        });
    }
    

    fillMapperTable(colName){
        return new Promise(async (resolve, reject) => {
            this.db.all(`SELECT DISTINCT ${colName} FROM ${this.SOURCE_TABLE_NAME}`, (err, rows) => {
                if(err) {
                    console.error(`could not retrieve rows`)
                    return reject(err)
                }
                const tableName = `oa_trf_src_${colName}_lkp`;
                const insertRow = this.db.prepare(`INSERT INTO ${tableName} (champ) VALUES(?)`)

                console.log(`fill mapper table ${colName} in progress ...`)

                const insertPromises = rows.map(row =>{
                    return new Promise((resolveInsert, rejectInsert) =>{
                        insertRow.run(row[colName], err =>{
                            if(err){
                                return rejectInsert(err)
                            }
                            resolveInsert()
                        })
                    })
                })
                
                Promise.all(insertPromises)
                .then(() => {
                    insertRow.finalize();
                    console.log(`fill mapper ${colName} table done ^^`);
                    resolve();
                })
                .catch((err)=>{
                    console.error(`Insertion failed ${colName}`)
                    reject(err)
                })  
            })
        })
    }

    createReducerTable(){
        return new Promise((resolve, reject) => {
            this.db.all(`PRAGMA table_info(${this.SOURCE_TABLE_NAME})`, (err, rows) => {
                if(err) {
                    console.error('Error in Pragma')
                    return reject(err)
                }

                const newCols = rows.map(row => {
                    if(this.allCols.includes(row.name)){
                        return `${row.name} ${row.type}`
                    }
                }).filter(Boolean)

                const newColsStr = newCols.join(',')

                this.db.run(`CREATE TABLE ${this.REDUCED_TABLE_NAME} (${newColsStr})`, (err, rows) => {
                    if(err) {
                        console.error('Error During Create')
                        return reject(err)
                    }
                    console.log('Reducer Table created successfully')
                    resolve()
                })
            })
        })
    }

    /**
     * Methode pour remplir la table de correspondance
     * @returns {Promise}
     */
    fillReducedTable = async () => {
        const batchSize = 2000; 
        let offset = 0; 

        console.log('Start filling reducer table ...')

        // Boucle pour traiter les données en lots
        while (true) {
            // Récupérer un lot de lignes de la table d'origine
            const rows = await new Promise((resolve, reject) => {
                this.db.all(`SELECT * FROM ${this.SOURCE_TABLE_NAME} LIMIT ${batchSize} OFFSET ${offset}`, (err, rows) => {
                    if (err) reject(err);
                    resolve(rows);
                });
            });

            // Si aucune ligne n'est retournée, toutes les lignes ont été traitées
            if (rows.length === 0) {
                break;
            }

            // Insérer le lot actuel de lignes dans la nouvelle table
            await this.insertBatch(rows);

            // Mettre à jour l'offset pour le prochain lot
            offset += batchSize;
        }

        console.log('Reduced table filling finished ^^.');
    };

    /**
     * Methode pour insérer un lot de lignes dans la nouvelle table
     * @param {*} rows 
     * @returns 
     */
    insertBatch = (rows) => {
        return new Promise((resolve, reject) => {
            const colNames = this.allCols.join(', ');
            const colValues = this.allCols.map(_ => '?').join(', ');

            // Début de la transaction SQLite
            this.db.run('BEGIN TRANSACTION');

            // Parcours de chaque ligne du lot
            rows.forEach(row => {
                // Préparation de l'instruction INSERT pour chaque ligne
                const insertStmt = this.db.prepare(`INSERT INTO ${this.REDUCED_TABLE_NAME}(${colNames}) VALUES (${colValues})`);

                const promises = this.allCols.map((col) => {
                    return new Promise((rowResolve, rowReject) => {
                        if (this.unmodifiedCols.includes(col)) {
                            rowResolve(row[col]);
                            return;
                        }

                        const tableName = `oa_trf_src_${col}_lkp`;
                        this.db.get(`SELECT id FROM ${tableName} WHERE champ = ?`, [row[col]], (err, rowReduced) => {
                            if (err) rowReject(err);
                            rowResolve(rowReduced?.id);
                        });
                    });
                });

                Promise.all(promises)
                    .then((ids) => {
                        insertStmt.run(ids, function(err) {
                            if (err) {
                                console.error('Insert error:', err);
                            }
                            // Finalisation de l'instruction INSERT pour chaque ligne
                            insertStmt.finalize();
                        });
                    })
                    .catch((err) => {
                        console.error('Promise.all error:', err);
                    });
            });

            // Commit de la transaction
            this.db.run('COMMIT', (err) => {
                if (err) {
                    this.db.run('ROLLBACK');
                    reject(err);
                } else {
                    resolve();
                }
            });
        });
    };

    async run(){
        if(!this.db) return;
    
        try {
            await this.getCols();
            await this.createMapperTable();
            for(const colname of this.colsToReduce){
                await this.fillMapperTable(colname)
            }
            await this.createReducerTable();
            await this.fillReducedTable()
        } catch (err) {
            console.error(err);
        } finally {
            this.db.close();
        }
    }
    
}

module.exports = TableReducer;