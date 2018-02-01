import { ConnectionPool, config, Request, Transaction as MSSQLTransaction } from "mssql";
import {
    Condition,
    Database, DatabaseError, Err, IDatabaseConfig, IModelCollection, IQueryOption, IQueryResult,
    ISchemaList, IUpsertResult, Vql, FieldType, RelationType, Model, IDeleteResult, Field, Schema, IModelFields,
    IFieldProperties,
    Transaction
} from "@vesta/core";
import { isUndefined } from "util";

interface ICalculatedQueryOptions {
    limit: string,
    orderBy: string,
    fields: string,
    fieldsList: Array<string>,
    condition: string,
    join: string,
}

export class MSSQL implements Database {


    private pool: ConnectionPool;
    private schemaList: ISchemaList = {};
    private config: IDatabaseConfig;
    private models: IModelCollection;
    private primaryKeys: { [name: string]: string } = {};
    private quote = '<#quote#>';

    private getConnection(transaction?: MSSQLTransaction) {
        if (transaction) return new Request(transaction)
        return new Request(this.pool);
    }



    public connect(): Promise<Database> {
        if (this.pool) return Promise.resolve(this);
        return new Promise<Database>((resolve, reject) => {
            this.pool = new ConnectionPool(<config>{
                server: this.config.host,
                port: +this.config.port,
                user: this.config.user,
                password: this.config.password,
                database: this.config.database,
                connectionTimeout: 600000,
                requestTimeout: 600000,
                pool: {
                    min: 5,
                    max: 100,
                    idleTimeoutMillis: 1000,
                }
            });
            this.pool.connect((err) => {
                if (err) {
                    return reject(new DatabaseError(Err.Code.DBConnection, err.message));
                }
                resolve(this);

            })
        })
    }

    constructor(config: IDatabaseConfig, models: IModelCollection) {
        this.schemaList = {};
        this.models = {};
        for (let model in models) {
            if (models.hasOwnProperty(model)) {
                this.schemaList[models[model].schema.name] = models[model].schema;
                this.models[models[model].schema.name] = models[model];
                this.pk(models[model].schema.name)
            }
        }

        this.config = config;
    }

    private pk(modelName): string {
        let pk = 'id';
        if (this.primaryKeys[modelName]) {
            return this.primaryKeys[modelName]
        } else {
            let fields = this.schemaList[modelName].getFields();
            for (let i = 0, keys = Object.keys(fields), il = keys.length; i < il; i++) {
                if (fields[keys[i]].properties.primary) {
                    pk = keys[i];
                    break;
                }
            }
        }
        this.primaryKeys[modelName] = pk;
        return pk;
    }

    public init(): Promise<any> {
        let createSchemaPromise: Promise<any> = this.initializeDatabase();
        for (let i = 0, schemaNames = Object.keys(this.schemaList), il = schemaNames.length; i < il; i++) {
            createSchemaPromise = createSchemaPromise.then(this.createTable(this.schemaList[schemaNames[i]]));
        }
        return createSchemaPromise;
    }

    private getTransaction(): Promise<MSSQLTransaction> {
        return new Promise<MSSQLTransaction>((resolve, reject) => {
            resolve(new MSSQLTransaction(this.pool))
        })
    }

    private prepareTransaction(transaction?: Transaction): Promise<Transaction> {
        if (!transaction) transaction = new Transaction();
        if (transaction.connection) return Promise.resolve(transaction);
        const sqlTr = new MSSQLTransaction(this.pool)
        return this.getTransaction().then(connection => {
            transaction.connection = connection;
            transaction.commit = () => new Promise((resolve, reject) => connection.commit((err) => {
                // if (err && err.fatal) {
                //     this.close(connection).then(() => reject(err)).catch(() => reject(err));
                // }
                err ? reject(err) : resolve(true)
            }));
            transaction.rollback = () => new Promise((resolve, reject) => connection.rollback(() => {
                // connection.release();
                resolve(true)
            }));
            return new Promise<Transaction>((resolve, reject) => connection.begin(undefined, (err) => err ? reject(err) : resolve(transaction)))
        })
    }

    private findById<T>(model: string, id: number | string, option: IQueryOption = {}, transaction?: Transaction): Promise<IQueryResult<T>> {
        let query = new Vql(model);
        query.where(new Condition(Condition.Operator.EqualTo).compare(this.pk(model), id));
        if (option.fields) query.select(...option.fields);
        if (option.relations) query.fetchRecordFor(...option.relations);
        query.orderBy = option.orderBy || [];
        query.limitTo(1);
        return this.findByQuery(query, transaction);
    }

    private findByModelValues<T>(model: string, modelValues: T, option: IQueryOption = {}, transaction?: Transaction): Promise<IQueryResult<T>> {
        let condition = new Condition(Condition.Operator.And);
        for (let i = 0, keys = Object.keys(modelValues), il = keys.length; i < il; i++) {
            if (modelValues[keys[i]] !== undefined) {
                condition.append((new Condition(Condition.Operator.EqualTo)).compare(keys[i], modelValues[keys[i]]));
            }
        }
        let query = new Vql(model);
        if (option.fields) query.select(...option.fields);
        if (option.offset || option.page) query.fromOffset(option.offset ? option.offset : (option.page - 1) * option.limit);
        if (option.relations) query.fetchRecordFor(...option.relations);
        if (+option.limit) query.limitTo(option.limit);
        query.where(condition);
        query.orderBy = option.orderBy || [];
        return this.findByQuery(query, transaction);
    }

    private findByQuery<T>(query: Vql, transaction?: Transaction): Promise<IQueryResult<T>> {
        let params: ICalculatedQueryOptions = this.getQueryParams(query);
        let result: IQueryResult<T> = <IQueryResult<T>>{};
        params.condition = params.condition ? 'WHERE ' + params.condition : '';
        params.orderBy = params.orderBy ? 'ORDER BY ' + params.orderBy : '';
        return this.query<Array<T>>(`SELECT ${params.fields} FROM [${query.model}] ${params.join} ${params.condition} ${params.orderBy} ${params.limit}`, null, transaction)
            .then(list => {
                return Promise.all([
                    this.getManyToManyRelation(list, query, transaction),
                    this.getLists(list, query, transaction)
                ]).then(() => list)
            })
            .then(list => {
                result.items = this.normalizeList(this.schemaList[query.model], list);
                result.total = result.items.length;
                return result;
            })
            .catch(err => {
                if (err) {
                    return Promise.reject(new DatabaseError(Err.Code.DBQuery, err));
                }
            })
    }

    public count<T>(model: string, modelValues: T, option?: IQueryOption, transaction?: Transaction): Promise<IQueryResult<T>>
    public count<T>(query: Vql, transaction?: Transaction): Promise<IQueryResult<T>>
    public count<T>(arg1: string | Vql, modelValues?: T, option?: IQueryOption, transaction?: Transaction): Promise<IQueryResult<T>> {
        let prepare: Promise<any> = transaction ? this.prepareTransaction(transaction) : Promise.resolve({});
        return prepare.then(() => {
            if ('string' == typeof arg1) {
                return this.countByModelValues<T>(<string>arg1, modelValues, option, transaction);
            } else {
                return this.countByQuery<T>(<Vql>arg1, transaction);
            }
        });
    }

    public find<T>(query: Vql, transaction?: Transaction): Promise<IQueryResult<T>>
    public find<T>(model: string, id: number | string, option?: IQueryOption, transaction?: Transaction): Promise<IQueryResult<T>>
    public find<T>(model: string, modelValues: T, option?: IQueryOption, transaction?: Transaction): Promise<IQueryResult<T>>
    public find<T>(arg1: string | Vql, arg2?: number | string | T, arg3?: IQueryOption, transaction?: Transaction): Promise<IQueryResult<T>> {
        let prepare: Promise<IQueryResult<T> | Transaction> = transaction ? this.prepareTransaction(transaction) : Promise.resolve(<Promise<IQueryResult<T>>>{});
        return prepare.then(() => {
            if ('string' == typeof arg1) {
                if (+arg2) return this.findById<T>(arg1, <number | string>arg2, arg3, transaction);
                else return this.findByModelValues<T>(arg1, <T>arg2, arg3, transaction);
            } else {
                return this.findByQuery<T>(<Vql>arg1, transaction);
            }
        })
    }


    public increase<T>(model: string, id: number | string, field: string, value: number, transaction?: Transaction): Promise<IUpsertResult<T>> {
        let start: Promise<Transaction> = !transaction ? Promise.resolve(null) : this.prepareTransaction(transaction);
        return start.then(transaction => this.query(`UPDATE \`${model}\` SET \`${field}\` = \`${field}\` + (?) WHERE ${this.pk(model)} = ?`, [value, id], transaction))
            .then(data => {
                return <Promise<IUpsertResult<T>>>this.findById<T>(model, id)
            })
    }

    private countByModelValues<T>(model: string, modelValues: T, option: IQueryOption = {}, transaction?: Transaction): Promise<IQueryResult<T>> {
        let condition = new Condition(Condition.Operator.And);
        for (let i = 0, keys = Object.keys(modelValues), il = keys.length; i < il; i++) {
            condition.append((new Condition(Condition.Operator.EqualTo)).compare(keys[i], modelValues[keys[i]]));
        }
        let query = new Vql(model);
        if (option.fields) query.select(...option.fields);
        if (option.offset || option.page) query.fromOffset(option.offset ? option.offset : (option.page - 1) * option.limit);
        if (option.relations) query.fetchRecordFor(...option.relations);
        if (+option.limit) query.limitTo(option.limit);
        query.where(condition);
        query.orderBy = option.orderBy || [];
        return this.countByQuery(query, transaction);
    }

    private countByQuery<T>(query: Vql, transaction?: Transaction): Promise<IQueryResult<T>> {
        let result: IQueryResult<T> = <IQueryResult<T>>{};
        let params: ICalculatedQueryOptions = this.getQueryParams(query);
        params.condition = params.condition ? 'WHERE ' + params.condition : '';
        return this.query(`SELECT COUNT(*) as total FROM [${query.model}] ${params.join} ${params.condition}`, null, transaction)
            .then(data => {
                result.total = data[0]['total'];
                return result;
            })
    }

    public insert<T>(model: string, value: T, transaction?: Transaction): Promise<IUpsertResult<T>>
    public insert<T>(model: string, values: Array<T>, transaction?: Transaction): Promise<IUpsertResult<T>>
    public insert<T>(model: string, arg2: Array<T> | T, transaction?: Transaction): Promise<IUpsertResult<T>> {
        if (arg2 instanceof Array) {
            return this.insertAll<T>(model, <Array<T>>arg2, transaction)
        } else {
            return this.insertOne<T>(model, <T>arg2, transaction)
        }
    }


    private insertOne<T>(model: string, value: T, transaction?: Transaction): Promise<IUpsertResult<T>> {
        let localTransaction = !transaction;
        let prepare: Promise<Transaction> = this.prepareTransaction(transaction).then(tr => transaction = tr);
        let result: IUpsertResult<T> = <IUpsertResult<T>>{};
        let analysedValue = this.getAnalysedValue<T>(model, value);
        let fields = [];
        let values = [];
        for (let i = analysedValue.properties.length; i--;) {
            fields.push(analysedValue.properties[i].field);
            values.push(analysedValue.properties[i].value);
        }

        return prepare.then(transaction => this.query(`INSERT INTO [${model}] (${fields.join(',')}) VALUES (${values.join(',')});SELECT SCOPE_IDENTITY() as id;`, null, transaction))
            .then(insertResult => {
                let steps = [];
                let id = insertResult[0]['id'];
                for (let key in analysedValue.relations) {
                    if (analysedValue.relations.hasOwnProperty(key)) {
                        steps.push(this.addRelation(new this.models[model]({ id: insertResult['insertId'] }), key, analysedValue.relations[key], transaction));
                    }

                }
                for (let key in analysedValue.lists) {
                    if (analysedValue.lists.hasOwnProperty(key)) {
                        steps.push(this.addList(new this.models[model]({ id: insertResult['insertId'] }), key, analysedValue.lists[key], transaction));
                    }
                }
                id = insertResult['insertId'];
                return Promise.all(steps).then(() => this.query(`SELECT * FROM [${model}] WHERE ${this.pk(model)} = ${id}`, null, transaction));
            })
            .then(list => {
                result.items = <Array<T>>list;
                return localTransaction ? transaction.commit().then(() => result) : result
            })
            .catch(err => {
                let error = new Err(Err.Code.DBInsert, err && err.message);
                return localTransaction ? transaction.rollback().then(() => Promise.reject(error)) : Promise.reject(error);
            });
    }

    private updateList<T>(model: T, list, value, transaction: Transaction) {
        let modelName = model['schema'].name;
        let table = modelName + this.pascalCase(list) + 'List';
        return this.query(`DELETE FROM ${table} WHERE fk = ${model[this.pk(modelName)]}`, null, transaction).then(() => {
            return this.addList(model, list, value, transaction)
        })
    }

    private addList<T>(model: T, list: string, value: Array<any>, transaction: Transaction): Promise<any> {
        let modelName = model['schema'].name;
        if (!value || !value.length) {
            return Promise.resolve([]);
        }
        let values = value.reduce((prev, value, index, items) => {
            let result = prev;
            result += `(${model[this.pk(modelName)]} , ${this.escape(value)})`;
            if (index < items.length - 1) result += ',';
            return result
        }, '');
        let table = modelName + this.pascalCase(list) + 'List';
        return this.query(`INSERT INTO ${table} ([fk],[value]) VALUES ${values}`, null, transaction)

    }

    private insertAll<T>(model: string, value: Array<T>, transaction?: Transaction): Promise<IUpsertResult<T>> {
        let result: IUpsertResult<T> = <IUpsertResult<T>>{};
        let fields = this.schemaList[model].getFields();
        let fieldsName = [];
        let insertList = [];
        let pk = this.pk(model);
        for (let field in fields) {
            if (fields.hasOwnProperty(field) && fields[field].properties.type != FieldType.Relation || fields[field].properties.relation.type == RelationType.One2Many || fields[field].properties.relation.type == RelationType.One2One) {
                // escape primary key with empty value
                if (field != pk || value[0][pk]) {
                    fieldsName.push(field);
                }
            }
        }
        for (let i = value.length; i--;) {
            let insertPart = [];
            for (let j = 0, jl = fieldsName.length; j < jl; j++) {
                if (fields[fieldsName[j]].properties.type == FieldType.Object) {
                    insertPart.push(value[i].hasOwnProperty(fieldsName[j]) ? this.escape(JSON.stringify(value[i][fieldsName[j]])) : '\'\'');
                }
                else {
                    let itemValue = value[i][fieldsName[j]];
                    let isNum = false;
                    if ([FieldType.Number, FieldType.Integer, FieldType.Timestamp, FieldType.Relation, FieldType.Enum].indexOf(fields[fieldsName[j]].properties.type) >= 0) {
                        isNum = true;
                    }
                    insertPart.push(value[i].hasOwnProperty(fieldsName[j]) ? this.escape(itemValue) : (isNum ? 0 : '\'\''));
                }
            }
            insertList.push(`(${insertPart.join(',')})`);
        }

        if (!insertList.length) {
            result.items = [];
            return Promise.resolve(result);
        }

        let prepare: Promise<Transaction> = transaction ? this.prepareTransaction(transaction).then(tr => transaction = tr) : Promise.resolve(null);
        return prepare.then(transaction => this.query<Array<T>>(`INSERT INTO ${model} (${fieldsName.join(',')}) VALUES ${insertList.join(',')}`, null, transaction))
            .then(insertResult => {
                result.items = insertResult;
                return result;
            })
            .catch(err => {
                let error = new Err(Err.Code.DBInsert, err && err.message);
                return Promise.reject(error)
            });

    }

    private addRelation<T, M>(model: T, relation: string, value: number | Array<number> | M | Array<M>, transaction?: Transaction): Promise<IUpsertResult<M>> {
        let modelName = model.constructor['schema'].name;
        let fields = this.schemaList[modelName].getFields();
        if (fields[relation] && fields[relation].properties.type == FieldType.Relation && (value || +value === 0)) {
            switch (fields[relation].properties.relation.type) {
                case RelationType.One2Many:
                case RelationType.One2One:
                    return this.addOneToManyRelation(model, relation, value, transaction);
                case RelationType.Many2Many:
                    return this.addManyToManyRelation(model, relation, value, transaction);
                default:
                    return Promise.resolve(<IUpsertResult<M>>{ items: [] });
            }
        }
        return Promise.reject(new Err(Err.Code.DBInsert, 'error in adding relation'));
    }

    private removeRelation<T>(model: T, relation: string, condition?: Condition | number | Array<number>, transaction?: Transaction): Promise<any> {
        let modelName = model.constructor['schema'].name;
        let relatedModelName = this.schemaList[modelName].getFields()[relation].properties.relation.model.schema.name;
        let safeCondition: Condition;
        if (typeof condition == 'number') {
            safeCondition = new Condition(Condition.Operator.EqualTo);
            safeCondition.compare(this.pk(relatedModelName), condition);
        } else if (condition instanceof Array && condition.length) {
            safeCondition = new Condition(Condition.Operator.Or);
            for (let i = condition.length; i--;) {
                safeCondition.append((new Condition(Condition.Operator.EqualTo)).compare(this.pk(relatedModelName), condition[i]))
            }
        } else if (condition instanceof Condition) {
            safeCondition = <Condition>condition;
        }
        let fields = this.schemaList[modelName].getFields();
        if (fields[relation] && fields[relation].properties.type == FieldType.Relation) {
            switch (fields[relation].properties.relation.type) {
                case RelationType.One2Many:
                case RelationType.One2One:
                    return this.removeOneToManyRelation(model, relation, transaction);
                case RelationType.Many2Many:
                    return this.removeManyToManyRelation(model, relation, safeCondition, transaction);
                default:
                    return Promise.resolve({});
            }
        }
        return Promise.reject(new Err(Err.Code.DBDelete, 'error in removing relation'));
    }

    private updateRelations(model: Model, relation, relatedValues, transaction?: Transaction) {
        let modelName = model.constructor['schema'].name;
        let relatedModelName = this.schemaList[modelName].getFields()[relation].properties.relation.model.schema.name;
        let ids = [0];
        if (relatedValues instanceof Array) {
            for (let i = relatedValues.length; i--;) {
                if (relatedValues[i]) {
                    ids.push(typeof relatedValues[i] == 'object' ? relatedValues[i][this.pk(relatedModelName)] : relatedValues[i]);
                }
            }
        }
        return this.query(`DELETE FROM ${this.pascalCase(modelName)}Has${this.pascalCase(relation)} 
                    WHERE ${this.camelCase(modelName)} = ${model[this.pk(modelName)]}`, null, transaction)
            .then(() => {
                return this.addRelation(model, relation, ids, transaction)
            })
    }

    public update<T>(model: string, value: T, transaction?: Transaction): Promise<IUpsertResult<T>>
    public update<T>(model: string, newValues: T, condition: Condition, transaction?: Transaction): Promise<IUpsertResult<T>>
    public update<T>(model: string, value: T, arg3?: Condition | Transaction, arg4?: Transaction): Promise<IUpsertResult<T>> {
        if (arg3 instanceof Condition) {
            return this.updateAll(model, value, <Condition>arg3, <Transaction>arg4)
        } else {
            return this.updateOne(model, value, <Transaction>arg3)
        }
    }

    private updateOne<T>(model: string, value: T, transaction?: Transaction): Promise<IUpsertResult<T>> {
        let localTransaction = !transaction;
        let prepare: Promise<Transaction> = this.prepareTransaction(transaction).then(tr => transaction = tr);
        let result: IUpsertResult<T> = <IUpsertResult<T>>{};
        let analysedValue = this.getAnalysedValue<T>(model, value);
        let properties = [];
        for (let i = analysedValue.properties.length; i--;) {
            if (analysedValue.properties[i].field != this.pk(model)) {
                properties.push(`[${analysedValue.properties[i].field}] = ${analysedValue.properties[i].value}`);
            }
        }
        let id = value[this.pk(model)];
        let steps = [];
        let relationsNames = Object.keys(analysedValue.relations);
        let modelFields = this.schemaList[model].getFields();

        return prepare.then(transaction => {
            for (let i = relationsNames.length; i--;) {
                let relation = relationsNames[i];
                let relationValue = analysedValue.relations[relation];
                // todo check if it is required
                if (!relationValue && (relationValue !== 0)) continue;
                switch (modelFields[relation].properties.relation.type) {
                    case RelationType.One2Many:
                    case RelationType.One2One:
                        let fk = +relationValue;
                        if (!fk && 'object' == typeof relationValue) {
                            let relatedModelName = modelFields[relation].properties.relation.model.schema.name;
                            fk = +relationValue[this.pk(relatedModelName)];
                        }
                        if (fk || fk === 0) {
                            properties.push(`[${relation}] = ${fk}`);
                        }
                        break;
                    case RelationType.Many2Many:
                        if (!relationValue) continue;
                        steps.push(this.updateRelations(new this.models[model](value), relation, relationValue, transaction));
                        break;
                }
            }
            for (let key in analysedValue.lists) {
                if (analysedValue.lists.hasOwnProperty(key)) {
                    steps.push(this.updateList(new this.models[model]({ id: id }), key, analysedValue.lists[key], transaction));
                }
            }
            return Promise.all<any>(steps).then(() => transaction)
        })

            .then((transaction) => properties.length ? this.query<Array<T>>(`UPDATE [${model}] SET ${properties.join(',')} WHERE ${this.pk(model)} = ${id}`, null, transaction) : [])
            .then(() => localTransaction ? transaction.commit() : true)
            .then(() => (<Promise<IUpsertResult<T>>>this.findById<T>(model, id)))
            .catch(err => {
                let error = new Err(Err.Code.DBQuery, err && err.message);
                return localTransaction ? transaction.rollback().then(() => Promise.reject(error)) : Promise.reject(error)
            });

    }

    private updateAll<T>(model: string, newValues: T, condition: Condition, transaction?: Transaction): Promise<IUpsertResult<T>> {
        let localTransaction = !transaction;
        let prepare: Promise<Transaction> = this.prepareTransaction(transaction).then(tr => transaction = tr);
        let sqlCondition = this.getCondition(model, condition);
        let result: IUpsertResult<T> = <IUpsertResult<T>>{};
        let properties = [];
        for (let key in newValues) {
            if (newValues.hasOwnProperty(key) && this.schemaList[model].getFieldsNames().indexOf(key) >= 0 && key != this.pk(model)) {
                properties.push(`[${model}].${key} = '${newValues[key]}'`)
            }
        }
        return prepare.then(transaction => this.query<Array<T>>(`SELECT ${this.pk(model)} FROM [${model}] ${sqlCondition ? `WHERE ${sqlCondition}` : ''}`, null, transaction))
            .then(list => {
                let ids = [];
                for (let i = list.length; i--;) {
                    ids.push(list[i][this.pk(model)]);
                }
                if (!ids.length) return [];
                return this.query<any>(`UPDATE [${model}] SET ${properties.join(',')}  WHERE ${this.pk(model)} IN (${ids.join(',')})}`, null, transaction)
                    .then(updateResult => {
                        return this.query<Array<T>>(`SELECT * FROM [${model}] WHERE ${this.pk(model)} IN (${ids.join(',')})`, null, transaction)
                    })
            })
            .then(list => {
                result.items = list;
                return localTransaction ? transaction.commit().then(() => result) : result;
            })
            .catch(err => {
                let error = new Err(Err.Code.DBUpdate, err && err.message);
                return localTransaction ? transaction.rollback().then(() => Promise.reject(error)) : Promise.reject(error);
            });
    }

    public remove(model: string, id: number | string, transaction?: Transaction): Promise<IDeleteResult>
    public remove(model: string, condition: Condition, transaction?: Transaction): Promise<IDeleteResult>
    public remove(model: string, arg2: Condition | number | string, transaction?: Transaction): Promise<IDeleteResult> {
        if ('string' == typeof arg2 || 'number' == typeof arg2) {
            return this.deleteOne(model, <number | string>arg2, transaction);
        } else if (arg2 instanceof Condition) {
            return this.deleteAll(model, arg2, transaction);
        } else {
            return Promise.reject<IDeleteResult>({
                error: new Err(Err.Code.WrongInput, 'invalid delete request'),
                items: null
            });
        }
    }

    private deleteOne(model: string, id: number | string, transaction?: Transaction): Promise<IDeleteResult> {
        let localTransaction = !transaction;
        let prepare: Promise<Transaction> = this.prepareTransaction(transaction).then(tr => transaction = tr);
        let result: IDeleteResult = <IDeleteResult>{};
        let fields = this.schemaList[model].getFields();
        return prepare.then(transaction => this.query(`DELETE FROM [${model}] WHERE ${this.pk(model)} = ${id}`, null, transaction))
            .then(deleteResult => {
                let instance = new this.models[model]();
                instance[this.pk(model)] = id;
                let steps = [];
                for (let field in this.schemaList[model].getFields()) {
                    if (fields.hasOwnProperty(field) && fields[field].properties.type == FieldType.Relation) {
                        steps.push(this.removeRelation(instance, field, 0, transaction));
                    }
                }
                result.items = [id];
                return Promise.all(steps).then(() => result);
            })
            .then(result => localTransaction ? transaction.commit().then(() => result) : result)
            .catch(err => {
                let error = new Err(Err.Code.DBDelete, err && err.message);
                return localTransaction ? transaction.rollback().then(() => Promise.reject(error)) : Promise.reject(error)
            })
    }

    private deleteAll<T>(model: string, condition: Condition, transaction?: Transaction): Promise<IDeleteResult> {
        let localTransaction = !transaction;
        let prepare: Promise<Transaction> = this.prepareTransaction(transaction).then(tr => transaction = tr);

        let sqlCondition = this.getCondition(model, condition);
        let result: IDeleteResult = <IDeleteResult>{};
        return prepare.then(transaction => this.query<Array<T>>(`SELECT ${this.pk(model)} FROM [${model}] ${sqlCondition ? `WHERE ${sqlCondition}` : ''}`, null, transaction))
            .then(list => {
                let ids = [];
                for (let i = list.length; i--;) {
                    ids.push(list[i][this.pk(model)]);
                }
                if (!ids.length) return [];
                return this.query(`DELETE FROM [${model}] WHERE ${this.pk(model)} IN (${ids.join(',')})`, null, transaction)
                    .then(deleteResult => ids)
            })
            .then(ids => {
                result.items = ids;
                return localTransaction ? transaction.commit().then(() => result) : result;
            })
            .catch(err => {
                let error = new Err(Err.Code.DBDelete, err && err.message);
                return localTransaction ? transaction.rollback().then(() => Promise.reject(error)) : Promise.reject(error);
            })
    }

    private getAnalysedValue<T>(model: string, value: T) {
        let properties = [];
        let schemaFieldsName = this.schemaList[model].getFieldsNames();
        let schemaFields = this.schemaList[model].getFields();
        let relations = {};
        let lists = {};

        for (let key in value) {
            if (value.hasOwnProperty(key) && schemaFieldsName.indexOf(key) >= 0 && value[key] !== undefined) {
                if (schemaFields[key].properties.type == FieldType.Relation) {
                    relations[<string>key] = value[<string>key]
                } else if (schemaFields[key].properties.type == FieldType.List) {
                    lists[<string>key] = value[<string>key]
                } else {
                    let thisValue: any = schemaFields[key].properties.type == FieldType.Object ? JSON.stringify(value[key]) : value[key];
                    properties.push({ field: key, value: thisValue })
                }
            }
        }
        return {
            properties: properties,
            relations: relations,
            lists: lists,
        }
    }

    private getQueryParams(query: Vql, alias: string = query.model): ICalculatedQueryOptions {
        let params: ICalculatedQueryOptions = <ICalculatedQueryOptions>{};
        query.offset = query.offset ? query.offset : (query.page ? query.page - 1 : 0) * query.limit;
        params.limit = '';
        let defaultSort = false;
        if (+query.limit) {
            params.limit = ` OFFSET ${query.offset} ROWS FETCH NEXT ${query.limit} ROWS ONLY `;
        }
        params.orderBy = '';
        if (query.orderBy.length) {
            let orderArray = [];
            for (let i = 0; i < query.orderBy.length; i++) {
                if (this.models[query.model].schema.getField(query.orderBy[i].field)) {
                    orderArray.push(`[${alias}].${query.orderBy[i].field} ${query.orderBy[i].ascending ? 'ASC' : 'DESC'}`);
                }
            }
            params.orderBy = orderArray.join(',');
        } else if (params.limit) {
            params.orderBy = this.pk(query.model);
            defaultSort = true;
        }
        let fields: Array<string> = [];
        let modelFields = this.schemaList[query.model].getFields();
        if (query.fields && query.fields.length) {
            for (let i = 0; i < query.fields.length; i++) {
                if (query.fields[i] instanceof Vql) {
                    fields.push(this.getSubQuery(<Vql>query.fields[i]));
                }
                else if (modelFields[<string>query.fields[i]]) {
                    if (modelFields[<string>query.fields[i]].properties.type == FieldType.List) continue;
                    fields.push(`[${alias}].${query.fields[i]}`)
                }
            }
        } else {
            for (let key in modelFields) {
                if (modelFields.hasOwnProperty(key)) {
                    if (modelFields[key].properties.type == FieldType.List) continue;
                    if (modelFields[key].properties.type != FieldType.Relation) {
                        fields.push(`[${alias}].${modelFields[key].fieldName}`);
                    }
                    else if ((!query.relations || query.relations.indexOf(modelFields[key].fieldName) < 0)
                        && (modelFields[key].properties.relation.type == RelationType.One2Many || modelFields[key].properties.relation.type == RelationType.One2One)) {
                        fields.push(`[${alias}].${modelFields[key].fieldName}`);
                    }
                }
            }
        }

        for (let i = 0; i < query.relations.length; i++) {
            let relationName: string = typeof query.relations[i] == 'string' ? query.relations[i] : query.relations[i]['name'];
            let field: Field = modelFields[relationName];
            if (!field) {
                throw `FIELD ${relationName} NOT FOUND IN model ${query.model} as ${alias}`
            }
            let properties = field.properties;
            if (properties.type == FieldType.Relation) {
                if (properties.relation.type == RelationType.One2Many || properties.relation.type == RelationType.One2One) {
                    let modelFiledList = [];
                    let filedNameList = properties.relation.model.schema.getFieldsNames();
                    let relatedModelFields = properties.relation.model.schema.getFields();
                    for (let j = 0; j < filedNameList.length; j++) {

                        if (typeof query.relations[i] == 'string' || query.relations[i]['fields'].indexOf(filedNameList[j]) >= 0) {
                            if (relatedModelFields[filedNameList[j]].properties.type != FieldType.List && (relatedModelFields[filedNameList[j]].properties.type != FieldType.Relation ||
                                (relatedModelFields[filedNameList[j]].properties.relation.type == RelationType.One2One || relatedModelFields[filedNameList[j]].properties.relation.type == RelationType.One2Many))) {
                                modelFiledList.push(`'${this.quote}${filedNameList[j]}${this.quote}:','${this.quote}',c.${filedNameList[j]},'${this.quote}'`)
                            }
                        }
                    }
                    let name = properties.relation.model.schema.name;
                    modelFiledList.length && fields.push(`(SELECT CONCAT('{',${modelFiledList.join(',",",')},'}') FROM [${name}] as c WHERE c.${this.pk(name)} = [${alias}].${field.fieldName}  LIMIT 1) as ${field.fieldName}`)
                }
            }
        }
        params.condition = '';
        if (query.condition) {
            params.condition = this.getCondition(query.model, query.condition, alias);
            params.condition = params.condition ? params.condition : '';
        }
        params.join = '';
        if (query.joins && query.joins.length) {
            let joins = [];
            for (let i = 0; i < query.joins.length; i++) {
                let join = query.joins[i];
                let type = '';
                switch (join.type) {
                    case Vql.Join:
                        type = 'FULL OUTER JOIN';
                        break;
                    case Vql.LeftJoin:
                        type = 'LEFT JOIN';
                        break;
                    case Vql.RightJoin:
                        type = 'RIGHT JOIN';
                        break;
                    case Vql.InnerJoin:
                        type = 'INNER JOIN';
                        break;
                    default:
                        type = 'LEFT JOIN';
                }
                let modelsAlias = join.vql.model + '_' + join.field;// + '__' + Math.floor(Math.random() * 100).toString(); // creating alias need refactoring some part code so i ignored it for this time.
                if (this.models[alias].schema.getField(join.field) && this.models[join.vql.model]) {
                    joins.push(`${type} ${join.vql.model} as ${modelsAlias} ON (${alias}.${join.field} = ${modelsAlias}.${this.pk(join.vql.model)})`);
                    let joinParam = this.getQueryParams(join.vql, modelsAlias);
                    if (joinParam.fields) {
                        fields.push(joinParam.fields);
                    }
                    if (joinParam.condition) {
                        params.condition = params.condition ? `(${params.condition} AND ${joinParam.condition})` : joinParam.condition
                    }
                    if (joinParam.orderBy) {
                        params.orderBy = params.orderBy ? `${params.orderBy},${joinParam.orderBy}` : joinParam.orderBy;
                    }
                    if (joinParam.join) {
                        joins.push(joinParam.join)
                    }
                }
            }
            params.join = joins.join('\n');
        }
        params.fields = fields.join(',');
        params.fieldsList = fields;
        return params;
    }

    private getSubQuery<T>(query: Vql) {
        query.relations = []; //relations not handle in next version;
        query.joins = [];
        let params: ICalculatedQueryOptions = this.getQueryParams(query, query.model);
        params.condition = params.condition ? 'WHERE ' + params.condition : '';
        params.orderBy = params.orderBy ? 'ORDER BY ' + params.orderBy : '';
        let modelFiledList = [];
        for (let i = 0, il = params.fieldsList.length; i < il; i++) {
            let field = params.fieldsList[i].replace(`\`${query.model}\`.`, '');
            modelFiledList.push(`'${this.quote}${field}${this.quote}:','${this.quote}',${field},'${this.quote}'`)
        }
        let modelAs = query.model[0].toLowerCase() + query.model.substr(1, query.model.length - 1);
        return `(SELECT CONCAT('{',${modelFiledList.join(',",",')},'}') FROM \`${query.model}\` ${params.condition} ${params.orderBy} limit 1) as \`${modelAs}\``;
    }

    private getCondition(model: string, condition: Condition, alias: string = model) {
        model = condition.model || model;
        let operator = this.getOperatorSymbol(condition.operator);
        if (!condition.isConnector) {
            if (!this.models[model].schema.getField(condition.comparison.field)) {
                return '';
            }
            return `([${alias}].${condition.comparison.field} ${operator} ${condition.comparison.isValueOfTypeField ? condition.comparison.value : `${this.escape(isUndefined(condition.comparison.value.id) ? condition.comparison.value : +condition.comparison.value.id)}`})`;
        } else {
            let childrenCondition = [];
            for (let i = 0; i < condition.children.length; i++) {
                let childCondition = this.getCondition(model, condition.children[i]).trim();
                childCondition && childrenCondition.push(childCondition);
            }
            let childrenConditionStr = childrenCondition.join(` ${operator} `).trim();
            return childrenConditionStr ? `(${childrenConditionStr})` : '';
        }
    }

    private getManyToManyRelation(list: Array<any>, query: Vql, transaction?: Transaction) {
        let ids = [];

        for (let i = list.length; i--;) {
            ids.push(list[i][this.pk(query.model)]);
        }
        let relations: Array<Promise<any>> = [];
        if (ids.length && query.relations && query.relations.length) {
            for (let i = query.relations.length; i--;) {
                let relationName = typeof query.relations[i] == 'string' ? query.relations[i] : query.relations[i]['name'];
                let field = this.schemaList[query.model].getFields()[relationName];
                let relationship = field.properties.relation;
                if (relationship.type == RelationType.Many2Many) {
                    relations.push(this.runRelatedQuery(query, i, ids, transaction))
                } else if (relationship.type == RelationType.Reverse) {
                    let reverseField = this.getReverseRelation(query, field);
                    if (reverseField.properties.relation.type == RelationType.One2Many || reverseField.properties.relation.type == RelationType.One2One) {
                        relations.push(this.runReverseQueryOne2Many(query, i, ids, reverseField, transaction));
                    } else if (reverseField.properties.relation.type == RelationType.Many2Many) {
                        relations.push(this.runRelatedQueryMany2Many(query, i, ids, reverseField, transaction));
                    }

                }
            }
        }
        if (!relations.length) return Promise.resolve(list);
        return Promise.all(relations)
            .then(data => {
                let leftKey = this.camelCase(query.model);
                for (let i = data.length; i--;) {
                    for (let related in data[i]) {
                        if (data[i].hasOwnProperty(related)) {
                            let relationship = this.schemaList[query.model].getFields()[related].properties.relation;
                            let rightKey = this.camelCase(relationship.model.schema.name);
                            for (let k = list.length; k--;) {
                                let id = list[k][this.pk(query.model)];
                                list[k][related] = [];
                                for (let j = data[i][related].length; j--;) {
                                    if (id == data[i][related][j][this.camelCase(query.model)]) {
                                        let relatedData = data[i][related][j];
                                        relatedData[this.pk(relationship.model.schema.name)] = relatedData[rightKey];
                                        delete relatedData[rightKey];
                                        delete relatedData[leftKey];
                                        list[k][related].push(relatedData);
                                    }
                                }

                            }
                        }
                    }
                }
                return list;
            });

    }

    private runRelatedQuery(query: Vql, i: number, ids: Array<number>, transaction?: Transaction) {
        let relationName = typeof query.relations[i] == 'string' ? query.relations[i] : query.relations[i]['name'];
        let relationship = this.schemaList[query.model].getFields()[relationName].properties.relation;
        let fields = '*';
        if (typeof query.relations[i] != 'string') {
            for (let j = query.relations[i]['fields'].length; j--;) {
                query.relations[i]['fields'][j] = `m.${query.relations[i]['fields'][j]}`;
            }
            fields = query.relations[i]['fields'].join(',');
        }
        let leftKey = this.camelCase(query.model);
        let rightKey = this.camelCase(relationship.model.schema.name);
        return this.query(`SELECT ${fields},r.${leftKey},r.${rightKey}  FROM \`${relationship.model.schema.name}\` m 
                LEFT JOIN \`${query.model + 'Has' + this.pascalCase(relationName)}\` r 
                ON (m.${this.pk(relationship.model.schema.name)} = r.${rightKey}) 
                WHERE r.${leftKey} IN (?)`, [ids], transaction)
            .then(relatedList => {
                let result = {};
                result[relationName] = relatedList;
                return result;
            })


    };

    private runReverseQueryOne2Many(query: Vql, i: number, ids: Array<number>, reverseField: Field, transaction?: Transaction) {
        let relationName = typeof query.relations[i] == 'string' ? query.relations[i] : query.relations[i]['name'];
        let relationship = this.schemaList[query.model].getFields()[relationName].properties.relation;
        let fields = ['*'];
        if (typeof query.relations[i] != 'string') {
            fields = query.relations[i]['fields'];
        }
        let leftKey = this.camelCase(query.model);
        let rightKey = this.camelCase(relationship.model.schema.name);
        fields.push(`${reverseField.fieldName} as ${leftKey}`);
        fields.push(`${this.pk(relationship.model.schema.name)} as ${rightKey}`);
        return this.query(`SELECT ${fields.join(',')} FROM ${relationship.model.schema.name} WHERE ${reverseField.fieldName} IN (?)`, [ids], transaction)
            .then(list => {
                let data = {};
                data[relationName] = list;
                return data;
            })
    };

    private runRelatedQueryMany2Many(query: Vql, i: number, ids: Array<number>, reverseField: Field, transaction?: Transaction) {
        let relationName = typeof query.relations[i] == 'string' ? query.relations[i] : query.relations[i]['name'];
        let relationship = this.schemaList[query.model].getFields()[relationName].properties.relation;
        let fields = '*';
        if (typeof query.relations[i] != 'string') {
            for (let j = query.relations[i]['fields'].length; j--;) {
                query.relations[i]['fields'][j] = `m.${query.relations[i]['fields'][j]}`;
            }
            fields = query.relations[i]['fields'].join(',');
        }
        let leftKey = this.camelCase(query.model);
        let rightKey = this.camelCase(relationship.model.schema.name);
        return this.query(`SELECT ${fields},r.${leftKey},r.${rightKey}  FROM \`${relationship.model.schema.name}\` m 
                LEFT JOIN \`${relationship.model.schema.name + 'Has' + this.pascalCase(reverseField.fieldName)}\` r 
                ON (m.${this.pk(query.model)} = r.${rightKey}) 
                WHERE r.${leftKey} IN (?)`, [ids], transaction)
            .then(relatedList => {
                let result = {};
                result[relationName] = relatedList;
                return result;
            })


    };


    private getReverseRelation(query: Vql, field: Field): Field | null {
        let modelName = query.model;
        let relatedField = null;
        let relatedModel = field.properties.relation.model;
        let fields = relatedModel.schema.getFields();
        let keys = relatedModel.schema.getFieldsNames();
        for (let i = 0, il = keys.length; i < il; i++) {
            let properties = fields[keys[i]].properties;
            if (properties.type == FieldType.Relation && properties.relation.model.schema.name == modelName) {
                relatedField = fields[keys[i]];
                break;
            }
        }
        return relatedField;
    }

    private getLists(list: Array<any>, query: Vql, transaction?: Transaction) {
        let runListQuery = (listName) => {
            let name = query.model + this.pascalCase(listName) + 'List';
            return this.query(`SELECT * FROM [${name}] WHERE [fk] IN (${ids.join(',')})`, null, transaction)
                .then(listsData => {
                    return {
                        name: listName,
                        data: listsData
                    };
                })


        };
        let primaryKey = this.pk(query.model);
        let ids = [];
        for (let i = list.length; i--;) {
            ids.push(list[i][primaryKey]);
        }
        let promiseList: Array<Promise<any>> = [];
        if (ids.length) {
            let fields = this.schemaList[query.model].getFields();
            for (let keys = Object.keys(fields), i = 0, il = keys.length; i < il; i++) {
                let field = keys[i];
                if (fields[field].properties.type == FieldType.List && (!query.fields || !query.fields.length || query.fields.indexOf(field) >= 0)) {
                    promiseList.push(runListQuery(field))
                }
            }
        }
        if (!promiseList.length) return Promise.resolve(list);
        let listJson = {};
        for (let i = list.length; i--;) {
            listJson[list[i][primaryKey]] = list[i];
        }
        return Promise.all(promiseList)
            .then(data => {
                for (let i = data.length; i--;) {
                    let listName = data[i].name;
                    let listData = data[i].data;
                    for (let k = listData.length; k--;) {
                        let id = listData[k]['fk'];
                        listJson[id][listName] = listJson[id][listName] || [];
                        listJson[id][listName].push(listData[k]['value']);
                    }
                }
                return list;
            });
    }

    private normalizeList(schema: Schema, list: Array<any>) {
        let fields: IModelFields = schema.getFields();
        for (let i = list.length; i--;) {
            for (let key in list[i]) {
                if (list[i].hasOwnProperty(key) &&
                    fields.hasOwnProperty(key) && (fields[key].properties.type == FieldType.Object || (
                        fields[key].properties.type == FieldType.Relation &&
                        (fields[key].properties.relation.type == RelationType.One2Many
                            || fields[key].properties.relation.type == RelationType.One2One)))) {
                    list[i][key] = this.parseJson(list[i][key], fields[key].properties.type == FieldType.Object);
                } else if (list[i].hasOwnProperty(key) && !fields.hasOwnProperty(key)) {
                    let isObject = list[i][key] && list[i][key].indexOf && list[i][key].indexOf(this.quote) < 0;
                    list[i][key] = this.parseJson(list[i][key], isObject);
                }
            }
        }
        return list;
    }

    private parseJson(str, isObject = false) {
        if (typeof str == 'string' && str) {
            let json;
            try {
                if (!isObject) {
                    let replace = ['\\n', '”', '\\r', '\\t', '\\v', "’", '"'];
                    let search = [/\n/ig, /"/ig, /\r/ig, /\t/ig, /\v/ig, /'/ig, new RegExp(this.quote, 'gi')];
                    for (let i = 0; i < search.length; i++) {
                        str = str.replace(search[i], replace[i]);
                    }
                }
                json = JSON.parse(str);
            } catch (e) {
                json = str;
            }
            return json
        } else {
            return str;
        }
    }

    private createTable(schema: Schema) {
        let fields = schema.getFields();
        let createDefinition = this.createDefinition(fields, schema.name);
        let ownTablePromise =
            this.query(`IF OBJECT_ID('${schema.name}', 'U') IS NOT NULL DROP TABLE ${schema.name}`)
                .then(() => {
                    return this.query(`CREATE TABLE [${schema.name}] (\n${createDefinition.ownColumn})\n`)
                });
        let translateTablePromise: Promise<any> = Promise.resolve(true);
        if (createDefinition.lingualColumn) {
            translateTablePromise =
                this.query(`IF OBJECT_ID('${schema.name}_translation', 'U') DROP TABLE ${schema.name}_translation`)
                    .then(() => {
                        return this.query(`CREATE TABLE ${schema.name}_translation (\n${createDefinition.lingualColumn}\n)`)
                    });
        }


        return () => Promise.all([ownTablePromise, translateTablePromise].concat(createDefinition.relations));

    }

    private relationTable(field: Field, table: string): Promise<any> {
        let name = table + 'Has' + this.pascalCase(field.fieldName);
        let schema = new Schema(name);
        schema.addField('id').primary().type(FieldType.Integer).required();
        schema.addField(this.camelCase(table)).type(FieldType.Integer).required();
        schema.addField(this.camelCase(field.properties.relation.model.schema.name)).type(FieldType.Integer).required();
        this.schemaList[name] = schema;
        return this.createTable(schema)();
    }

    private listTable(field: Field, table: string): Promise<any> {
        let name = table + this.pascalCase(field.fieldName) + 'List';
        let schema = new Schema(name);
        schema.addField('id').primary().type(FieldType.Integer).required();
        schema.addField('fk').type(FieldType.Integer).required();
        schema.addField('value').type(field.properties.list).required();
        this.schemaList[name] = schema;
        return this.createTable(schema)();
    }

    private camelCase(str) {
        return str[0].toLowerCase() + str.slice(1)
    }

    private pascalCase(str) {
        return str[0].toUpperCase() + str.slice(1)
    }

    private createDefinition(fields: IModelFields, table: string, checkMultiLingual = true) {
        let multiLingualDefinition: Array<String> = [];
        let columnDefinition: Array<String> = [];
        let relations: Array<Promise<boolean>> = [];
        let keyIndex;
        for (let field in fields) {
            if (fields.hasOwnProperty(field)) {
                keyIndex = fields[field].properties.primary ? field : keyIndex;
                let column = this.columnDefinition(fields[field]);
                if (column) {
                    if (fields[field].properties.multilingual && checkMultiLingual) {
                        multiLingualDefinition.push(column);
                    } else {
                        columnDefinition.push(column);
                    }
                } else if (fields[field].properties.type == FieldType.Relation && fields[field].properties.relation.type == RelationType.Many2Many) {
                    relations.push(this.relationTable(fields[field], table));
                } else if (fields[field].properties.type == FieldType.List) {
                    relations.push(this.listTable(fields[field], table));
                }
            }
        }
        let keyFiled;

        if (keyIndex) {
            keyFiled = fields[keyIndex];
        } else {
            keyFiled = new Field('id');
            keyFiled.primary().type(FieldType.Integer).required();
            columnDefinition.push(this.columnDefinition(keyFiled));
        }

        let keySyntax = `PRIMARY KEY (${keyFiled.fieldName})`;
        columnDefinition.push(keySyntax);

        if (multiLingualDefinition.length) {
            multiLingualDefinition.push(this.columnDefinition(keyFiled));
            multiLingualDefinition.push(keySyntax);
        }

        return {
            ownColumn: columnDefinition.join(' ,\n '),
            lingualColumn: multiLingualDefinition.join(' ,\n '),
            relations: relations
        }
    }

    private columnDefinition(filed: Field) {
        let properties = filed.properties;
        if (properties.type == FieldType.List || (properties.relation && properties.relation.type == RelationType.Many2Many) || (properties.relation && properties.relation.type == RelationType.Reverse)) {
            return '';
        }
        let defaultRelation;
        if (properties.relation && (properties.relation.type == RelationType.One2One || properties.relation.type == RelationType.One2Many)) {
            defaultRelation = true;
        }
        let defaultValue = properties.type != FieldType.Boolean ? `'${defaultRelation ? 0 : properties.default}'` : !!properties.default;
        let columnSyntax = `\`${filed.fieldName}\` ${this.getType(properties)}`;
        columnSyntax += (properties.required && properties.type != FieldType.Relation) || properties.primary ? ' NOT NULL' : '';
        columnSyntax += properties.default || properties.default === 0 || properties.default === '' || defaultRelation ? ` DEFAULT '${defaultValue}'` : '';
        columnSyntax += properties.unique ? ' UNIQUE ' : '';
        columnSyntax += properties.primary ? ' IDENTITY(1,1) ' : '';
        return columnSyntax;
    }

    private getType(properties: IFieldProperties) {
        let typeSyntax;
        switch (properties.type) {
            case FieldType.Boolean:
                typeSyntax = "BIT";
                break;
            case FieldType.EMail:
            case FieldType.File:
            case FieldType.Password:
            case FieldType.Tel:
            case FieldType.URL:
            case FieldType.String:
                if (!properties.primary) {
                    typeSyntax = `NVARCHAR(${properties.maxLength ? properties.maxLength : 255})`;
                } else {
                    typeSyntax = 'BIGINT';
                }
                break;
            case FieldType.Float:
            case FieldType.Number:
                typeSyntax = `DECIMAL(${properties.max ? properties.max.toString().length : 10},10)`;
                break;
            case FieldType.Enum:
            case FieldType.Integer:
                typeSyntax = `INT`;
                break;
            case FieldType.Object:
            case FieldType.Text:
                typeSyntax = `NTEXT`;
                break;
            case FieldType.Timestamp:
                typeSyntax = 'BIGINT';
                break;
            case FieldType.Relation:
                if (properties.relation.type == RelationType.One2One || properties.relation.type == RelationType.One2Many) {
                    typeSyntax = 'BIGINT';
                }
                break;

        }
        return typeSyntax;
    }

    private initializeDatabase() {
        return Promise.resolve();
        // return this.query(`ALTER DATABASE [${this.config.database}]  CHARSET = utf8 COLLATE = utf8_general_ci;`);
    }

    private getOperatorSymbol(operator: number): string {
        switch (operator) {
            // Connectors
            case Condition.Operator.And:
                return 'AND';
            case Condition.Operator.Or:
                return 'OR';
            // Comparison
            case Condition.Operator.EqualTo:
                return '=';
            case Condition.Operator.NotEqualTo:
                return '<>';
            case Condition.Operator.GreaterThan:
                return '>';
            case Condition.Operator.GreaterThanOrEqualTo:
                return '>=';
            case Condition.Operator.LessThan:
                return '<';
            case Condition.Operator.LessThanOrEqualTo:
                return '<=';
            case Condition.Operator.Like:
                return 'LIKE';
            case Condition.Operator.NotLike:
                return 'NOT LIKE';
        }
    }

    private addOneToManyRelation<T, M>(model: T, relation: string, value: number | { [property: string]: any }, transaction?: Transaction): Promise<IUpsertResult<M>> {
        let result: IUpsertResult<T> = <IUpsertResult<T>>{};
        let modelName = model.constructor['schema'].name;
        let fields = this.schemaList[modelName].getFields();
        let relatedModelName = fields[relation].properties.relation.model.schema.name;
        let readIdPromise;
        if (fields[relation].properties.relation.isWeek && typeof value == 'object' && !value[this.pk(relatedModelName)]) {
            readIdPromise = this.insertOne(relatedModelName, value, transaction).then(result => result.items[0][this.pk(relatedModelName)])
        } else {
            let id = 0;
            if (+value || +value === 0) {
                id = +value;
            } else if (typeof value == 'object') {
                id = +value[this.pk(relatedModelName)]
            }
            if (id < 0) return Promise.reject(new Error(`invalid <<${relation}>> related model id`));
            readIdPromise = Promise.resolve(id);
        }
        return readIdPromise
            .then(id => {
                return this.query<Array<T>>(`UPDATE [${modelName}] SET [${relation}] = '${id}' WHERE ${this.pk(relatedModelName)}='${model[this.pk(relatedModelName)]}' `, null, transaction)
            })
            .then(updateResult => {
                result.items = updateResult;
                return result;
            })
            .catch(err => {
                return Promise.reject(new Err(Err.Code.DBUpdate, err && err.message));
            })

    }

    private addManyToManyRelation<T, M>(model: T, relation: string, value: number | Array<number> | M | Array<M>, transaction?: Transaction): Promise<IUpsertResult<M>> {
        let result: IUpsertResult<M> = <IUpsertResult<M>>{};
        let modelName = model.constructor['schema'].name;
        let fields = this.schemaList[modelName].getFields();
        let relatedModelName = fields[relation].properties.relation.model.schema.name;
        let newRelation = [];
        let relationIds = [];
        if (+value > 0) {
            relationIds.push(+value);
        } else if (value instanceof Array) {
            for (let i = value['length']; i--;) {
                if (+value[i]) {
                    relationIds.push(+value[i])
                } else if (value[i] && typeof value[i] == 'object') {
                    if (+value[i][this.pk(relatedModelName)]) relationIds.push(+value[i][this.pk(relatedModelName)]);
                    else if (fields[relation].properties.relation.isWeek) newRelation.push(value[i])
                }
            }
        } else if (typeof value == 'object') {
            if (+value[this.pk(relatedModelName)]) {
                relationIds.push(+value[this.pk(relatedModelName)])
            } else if (fields[relation].properties.relation.isWeek) newRelation.push(value)
        }
        return Promise.resolve()
            .then(() => {
                if (!newRelation.length) {
                    return relationIds;
                }
                return this.insertAll(relatedModelName, newRelation, transaction)
                    .then(result => {
                        for (let i = result.items.length; i--;) {
                            relationIds.push(result.items[i][this.pk(relatedModelName)]);
                        }
                        return relationIds;
                    })

            })
            .then(relationIds => {
                if (!relationIds || !relationIds.length) {
                    result.items = [];
                    return result;
                }
                let insertList = [];
                for (let i = relationIds.length; i--;) {
                    insertList.push(`(${model[this.pk(modelName)]},${this.escape(relationIds[i])})`);
                }
                return this.query<any>(`INSERT INTO ${modelName}Has${this.pascalCase(relation)}
                    ([${this.camelCase(modelName)}],[${this.camelCase(relatedModelName)}]) VALUES ${insertList.join(',')}`, null, transaction)
                    .then(insertResult => {
                        result.items = insertResult;
                        return result
                    })

            })
            .catch(err => {
                return Promise.reject(new Err(Err.Code.DBInsert, err && err.message));
            });

    }

    private removeOneToManyRelation<T>(model: T, relation: string, transaction: Transaction) {
        let modelName = model.constructor['schema'].name;
        let result: IUpsertResult<T> = <IUpsertResult<T>>{};
        let relatedModelName = this.schemaList[modelName].getFields()[relation].properties.relation.model.schema.name;
        let isWeek = this.schemaList[modelName].getFields()[relation].properties.relation.isWeek;
        let preparePromise: Promise<number> = Promise.resolve(0);
        if (isWeek) {
            let readRelationId: Promise<number> = +model[relation] ? Promise.resolve(+model[relation]) : this.findById(modelName, model[this.pk(modelName)]).then(result => result.items[0][relation]);
            readRelationId.then(relationId => {
                return this.deleteOne(relatedModelName, relationId, transaction).then(() => relationId);
            })
        }
        return preparePromise
            .then(() => {
                return this.query<any>(`UPDATE [${modelName}] SET ${relation} = 0 WHERE ${this.pk(modelName)} = ${this.escape(model[this.pk(modelName)])}`, null, transaction)
                    .then(updateResult => {
                        result.items = updateResult;
                        return result;
                    })

            })
            .catch(err => {
                return Promise.reject(new Err(Err.Code.DBUpdate, err && err.message))
            })

    }

    private removeManyToManyRelation<T>(model: T, relation: string, condition: Condition, transaction: Transaction): Promise<any> {
        let modelName = model.constructor['schema'].name;
        let relatedModelName = this.schemaList[modelName].getFields()[relation].properties.relation.model.schema.name;
        let isWeek = this.schemaList[modelName].getFields()[relation].properties.relation.isWeek;
        let preparePromise: Promise<any>;
        if (condition) {
            let vql = new Vql(relatedModelName);
            vql.select(this.pk(relatedModelName)).where(condition);
            preparePromise = this.findByQuery(vql)
        } else {
            preparePromise = Promise.resolve();
        }

        return preparePromise
            .then(result => {
                let conditions = [];
                let conditionsStr;
                let relatedField = this.camelCase(relatedModelName);
                if (result && result.items.length) {
                    for (let i = result.items.length; i--;) {
                        result.items.push(+result.items[0][this.pk(relatedModelName)]);
                        conditions.push(`${relatedField} = '${+result.items[0][this.pk(relatedModelName)]}'`)
                    }
                } else if (result) {
                    conditions.push('FALSE');
                }
                conditionsStr = conditions.length ? ` AND ${conditions.join(' OR ')}` : '';
                return this.query<Array<any>>(`SELECT * FROM ${modelName + 'Has' + this.pascalCase(relation)} WHERE ${this.camelCase(modelName)} = ${model[this.pk(modelName)]} ${conditionsStr}`)
                    .then(items => {
                        let ids: Array<number> = [];
                        for (let i = items.length; i--;) {
                            ids.push(items[i][relatedField])
                        }
                        return ids;
                    })
            })
            .then(ids => {
                let relatedField = this.camelCase(relatedModelName);
                let idConditions = [];
                let condition = new Condition(Condition.Operator.Or);
                for (let i = ids.length; i--;) {
                    idConditions.push(`${relatedField} = '${+ids[i]}'`);
                    condition.append(new Condition(Condition.Operator.EqualTo).compare('id', ids[i]));
                }
                let idCondition = ids.length ? `(${ids.join(' OR ')})` : 'FALSE';
                return this.query(`DELETE FROM ${model + 'Has' + this.pascalCase(relation)} WHERE ${this.camelCase(modelName)} = ${model[this.pk(modelName)]} AND ${idCondition}}`, null, transaction)
                    .then(() => {
                        let result = { items: ids };
                        if (isWeek && ids.length) {
                            return this.deleteAll(relatedModelName, condition, transaction).then(() => result);
                        }
                        return result;
                    });
            });
    }

    public escape(value) {
        if (typeof value == 'number') return value;
        if (typeof value == 'boolean') return value ? 1 : 0;
        return `N'${value.replace('\'', '\'\'')}'`;
    }

    public query<T>(query: string, data?: Array<number | string | Array<number | string>>, transaction?: Transaction): Promise<T> {
        if (!transaction) {
            return new Promise((resolve, reject) => {
                let connection = this.getConnection()
                connection.query<T>(query, (err, result) => {
                    if (err) {
                        return reject(err);
                    } else {
                        resolve(<any>(result.recordset || result.output || result));
                    }
                })
            })
        } else {
            return this.prepareTransaction(transaction)
                .then(transaction => new Promise<T>((resolve, reject) => {
                    let connection = this.getConnection(transaction.connection);
                    connection.query(query, (err, result) => {
                        if (err) {
                            return reject(err);
                        } else {
                            resolve(<any>(result.recordset || result.output || result));
                        }
                    })
                }));

        }
    }


    public close(connection: any): Promise<boolean> {
        return new Promise((resolve, reject) => {
            this.pool.close((err) => {
                if (err) reject(err);
                else resolve(true);
            });
        })
    }

}