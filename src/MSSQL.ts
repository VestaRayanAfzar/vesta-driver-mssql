import {Connection, config, Request} from "mssql";
import {
    Condition,
    Database, DatabaseError, Err, IDatabaseConfig, IModelCollection, IQueryOption, IQueryResult,
    ISchemaList, IUpsertResult, Vql, FieldType, RelationType, Model, IDeleteResult, Field, Schema, IModelFields,
    IFieldProperties
} from "vesta-lib";

interface ICalculatedQueryOptions {
    limit: string,
    orderBy: string,
    fields: string,
    condition: string,
    join: string,
}

export class MSSQL extends Database {

    private SQLConnection: Connection;
    private schemaList: ISchemaList = {};
    private config: IDatabaseConfig;
    private models: IModelCollection;
    private primaryKeys: { [name: string]: string } = {};

    get connection() {
        return new Request(this.SQLConnection);
    }

    increase<T>(model: string, id: string | number, field: string, value: number): Promise<IQueryResult<T>> {
        throw new Error('Method not implemented.');
    }

    public connect(): Promise<Database> {
        if (this.SQLConnection) return Promise.resolve(this);
        return new Promise<Database>((resolve, reject) => {
            this.SQLConnection = new Connection(<config>{
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
            this.SQLConnection.connect((err) => {
                if (err) {
                    return reject(new DatabaseError(Err.Code.DBConnection, err.message));
                }
                resolve(this);

            })
        })
    }

    constructor(config: IDatabaseConfig, models: IModelCollection) {
        super();
        this.schemaList = {};
        for (let model in models) {
            if (models.hasOwnProperty(model)) {
                this.schemaList[model] = models[model].schema;
                this.pk(model)
            }
        }
        this.models = models;
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

    public init(): Promise<boolean> {
        let createSchemaPromise = this.initializeDatabase();
        for (let i = 0, schemaNames = Object.keys(this.schemaList), il = schemaNames.length; i < il; i++) {
            createSchemaPromise = createSchemaPromise.then(this.createTable(this.schemaList[schemaNames[i]]));
        }
        return createSchemaPromise;
    }

    public findById<T>(model: string, id: number | string, option: IQueryOption = {}): Promise<IQueryResult<T>> {
        let query = new Vql(model);
        query.where(new Condition(Condition.Operator.EqualTo).compare(this.pk(model), id));
        if (option.fields) query.select(...option.fields);
        if (option.relations) query.fetchRecordFor(...option.relations);
        query.orderBy = option.orderBy || [];
        query.limitTo(1);
        return this.findByQuery(query);
    }

    public findByModelValues<T>(model: string, modelValues: T, option: IQueryOption = {}): Promise<IQueryResult<T>> {
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
        return this.findByQuery(query);
    }

    public findByQuery<T>(query: Vql): Promise<IQueryResult<T>> {
        let params: ICalculatedQueryOptions = this.getQueryParams(query);
        let result: IQueryResult<T> = <IQueryResult<T>>{};
        params.condition = params.condition ? 'WHERE ' + params.condition : '';
        params.orderBy = params.orderBy ? 'ORDER BY ' + params.orderBy : '';
        return this.query<Array<T>>(`SELECT ${params.fields} FROM [${query.model}] ${params.join} ${params.condition} ${params.orderBy} ${params.limit}`)
            .then(list => {
                return Promise.all([
                    this.getManyToManyRelation(list, query),
                    this.getLists(list, query)
                ]).then(() => list)
            })
            .then(list => {
                result.items = this.normalizeList(this.schemaList[query.model], list);
                return result;
            })
            .catch(err => {
                if (err) {
                    return Promise.reject(new Err(Err.Code.DBQuery, err && err.message));
                }
            })
    }

    public count<T>(model: string, modelValues: T, option?: IQueryOption): Promise<IQueryResult<T>>
    public count<T>(query: Vql): Promise<IQueryResult<T>>
    public count<T>(arg1: string | Vql, modelValues?: T, option?: IQueryOption): Promise<IQueryResult<T>> {
        if ('string' == typeof arg1) {
            return this.countByModelValues(<string>arg1, modelValues, option);
        } else {
            return this.countByQuery(<Vql>arg1);
        }
    }

    private countByModelValues<T>(model: string, modelValues: T, option: IQueryOption = {}): Promise<IQueryResult<T>> {
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
        return this.countByQuery(query);
    }

    public countByQuery<T>(query: Vql): Promise<IQueryResult<T>> {
        let result: IQueryResult<T> = <IQueryResult<T>>{};
        let params: ICalculatedQueryOptions = this.getQueryParams(query);
        params.condition = params.condition ? 'WHERE ' + params.condition : '';
        return this.query(`SELECT COUNT(*) as total FROM [${query.model}] ${params.join} ${params.condition}`)
            .then(data => {
                result.total = data[0]['total'];
                return result;
            })
    }


    public insertOne<T>(model: string, value: T): Promise<IUpsertResult<T>> {
        let result: IUpsertResult<T> = <IUpsertResult<T>>{};
        let analysedValue = this.getAnalysedValue<T>(model, value);
        let fields = [];
        let values = [];
        for (let i = analysedValue.properties.length; i--;) {
            fields.push(analysedValue.properties[i].field);
            values.push(analysedValue.properties[i].value);
        }

        return this.query(`INSERT INTO [${model}] (${fields.join(',')}) VALUES (${values.join(',')});SELECT SCOPE_IDENTITY() as id;`)
            .then(insertResult => {
                let steps = [];
                let id = insertResult[0]['id'];
                for (let key in analysedValue.relations) {
                    if (analysedValue.relations.hasOwnProperty(key)) {
                        steps.push(this.addRelation(new this.models[model]({id: insertResult['insertId']}), key, analysedValue.relations[key]));
                    }

                }
                for (let key in analysedValue.lists) {
                    if (analysedValue.lists.hasOwnProperty(key)) {
                        steps.push(this.addList(new this.models[model]({id: insertResult['insertId']}), key, analysedValue.lists[key]));
                    }
                }
                id = insertResult['insertId'];
                return Promise.all(steps).then(() => this.query(`SELECT * FROM [${model}] WHERE ${this.pk(model)} = ${id}`));
            })
            .then(list => {
                result.items = <Array<T>>list;
                return result;
            })
            .catch(err => {
                result.error = new Err(Err.Code.DBInsert, err && err.message);
                return Promise.reject(result);
            });
    }

    private updateList<T>(model: T, list, value) {
        let modelName = model['schema'].name;
        let table = modelName + this.pascalCase(list) + 'List';
        return this.query(`DELETE FROM ${table} WHERE fk = ${model[this.pk(modelName)]}`).then(() => {
            return this.addList(model, list, value)
        })
    }

    private addList<T>(model: T, list: string, value: Array<any>): Promise<any> {
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
        return this.query(`INSERT INTO ${table} ([fk],[value]) VALUES ${values}`)

    }

    public insertAll<T>(model: string, value: Array<T>): Promise<IUpsertResult<T>> {
        let result: IUpsertResult<T> = <IUpsertResult<T>>{};
        let fields = this.schemaList[model].getFields();
        let fieldsName = [];
        let insertList = [];
        for (let field in fields) {
            if (fields.hasOwnProperty(field) && fields[field].properties.type != FieldType.Relation || fields[field].properties.relation.type != RelationType.Many2Many) {
                fieldsName.push(field);
            }
        }
        for (let i = value.length; i--;) {
            let insertPart = [];
            for (let j = 0, jl = fieldsName.length; j < jl; j++) {
                insertPart.push(value[i].hasOwnProperty(fieldsName[j]) ? this.escape(value[i][fieldsName[j]]) : '\'\'');
            }
            insertList.push(`(${insertPart.join(',')})`)

        }

        if (!insertList.length) {
            result.items = [];
            return Promise.resolve(result);
        }

        return this.query<Array<T>>(`INSERT INTO ${model} (${fieldsName.join(',')}) VALUES ${insertList.join(',')}`)
            .then(insertResult => {
                result.items = insertResult;
                return result;
            })
            .catch(err => {
                result.error = new Err(Err.Code.DBInsert, err && err.message);
                return Promise.reject(result)
            });

    }

    private addRelation<T, M>(model: T, relation: string, value: number
        | Array<number>
        | M
        | Array<M>): Promise<IUpsertResult<M>> {
        let modelName = model.constructor['schema'].name;
        let fields = this.schemaList[modelName].getFields();
        if (fields[relation] && fields[relation].properties.type == FieldType.Relation && value) {
            if (fields[relation].properties.relation.type != RelationType.Many2Many) {
                return this.addOneToManyRelation<T, M>(model, relation, value)
            } else {
                return this.addManyToManyRelation<T, M>(model, relation, value)
            }
        }
        return Promise.reject(new Err(Err.Code.DBInsert, 'error in adding relation'));
    }

    private removeRelation<T>(model: T, relation: string, condition?: Condition
        | number
        | Array<number>): Promise<any> {
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
            if (fields[relation].properties.relation.type != RelationType.Many2Many) {
                return this.removeOneToManyRelation(model, relation)
            } else {
                return this.removeManyToManyRelation(model, relation, safeCondition)
            }
        }
        return Promise.reject(new Err(Err.Code.DBDelete, 'error in removing relation'));
    }

    private updateRelations(model: Model, relation, relatedValues) {
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
                    WHERE ${this.camelCase(modelName)} = ${model[this.pk(modelName)]}`)
            .then(() => {
                return this.addRelation(model, relation, ids)
            })
    }

    public updateOne<T>(model: string, value: T): Promise<IUpsertResult<T>> {
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
        for (let i = relationsNames.length; i--;) {
            let relation = relationsNames[i];
            let relationValue = analysedValue.relations[relation];
            // todo check if it is required
            if (!relationValue) continue;
            if (modelFields[relation].properties.relation.type == RelationType.Many2Many) {
                steps.push(this.updateRelations(new this.models[model](value), relation, relationValue));
            } else {
                let fk = +relationValue;
                if (!fk && 'object' == typeof relationValue) {
                    let relatedModelName = modelFields[relation].properties.relation.model.schema.name;
                    fk = +relationValue[this.pk(relatedModelName)];
                }
                if (fk) {
                    properties.push(`\`${relation}\` = ${fk}`);
                }
            }
        }
        for (let key in analysedValue.lists) {
            if (analysedValue.lists.hasOwnProperty(key)) {
                steps.push(this.updateList(new this.models[model]({id: id}), key, analysedValue.lists[key]));
            }
        }

        return Promise.all(steps)
            .then(() => this.query<Array<T>>(`UPDATE [${model}] SET ${properties.join(',')} WHERE ${this.pk(model)} = ${id}`))
            .then(() => this.findById(model, id))
            .catch(err => {
                result.error = new Err(Err.Code.DBQuery, err && err.message);
                return Promise.reject(result);
            });

    }

    public updateAll<T>(model: string, newValues: T, condition: Condition): Promise<IUpsertResult<T>> {
        let sqlCondition = this.getCondition(model, condition);
        let result: IUpsertResult<T> = <IUpsertResult<T>>{};
        let properties = [];
        for (let key in newValues) {
            if (newValues.hasOwnProperty(key) && this.schemaList[model].getFieldsNames().indexOf(key) >= 0 && key != this.pk(model)) {
                properties.push(`[${model}].${key} = '${newValues[key]}'`)
            }
        }
        return this.query<Array<T>>(`SELECT ${this.pk(model)} FROM [${model}] ${sqlCondition ? `WHERE ${sqlCondition}` : ''}`)
            .then(list => {
                let ids = [];
                for (let i = list.length; i--;) {
                    ids.push(list[i][this.pk(model)]);
                }
                if (!ids.length) return [];
                return this.query<any>(`UPDATE [${model}] SET ${properties.join(',')}  WHERE ${this.pk(model)} IN (${ids.join(',')})}`)
                    .then(updateResult => {
                        return this.query<Array<T>>(`SELECT * FROM [${model}] WHERE ${this.pk(model)} IN (${ids.join(',')})`)
                    })
            })
            .then(list => {
                result.items = list;
                return result
            })
            .catch(err => {
                result.error = new Err(Err.Code.DBUpdate, err && err.message);
                return Promise.reject(result);
            });
    }

    public deleteOne(model: string, id: number | string): Promise<IDeleteResult> {
        let result: IDeleteResult = <IDeleteResult>{};
        let fields = this.schemaList[model].getFields();
        return this.query(`DELETE FROM [${model}] WHERE ${this.pk(model)} = ${id}`)
            .then(deleteResult => {
                for (let field in this.schemaList[model].getFields()) {
                    if (fields.hasOwnProperty(field) && fields[field].properties.type == FieldType.Relation) {
                        this.removeRelation(model, field, 0)
                    }
                }
                result.items = [id];
                return result;
            })
            .catch(err => {
                result.error = new Err(Err.Code.DBDelete, err && err.message);
                return Promise.reject(result);
            })
    }

    public deleteAll<T>(model: string, condition: Condition): Promise<IDeleteResult> {
        let sqlCondition = this.getCondition(model, condition);
        let result: IDeleteResult = <IDeleteResult>{};
        return this.query<Array<T>>(`SELECT ${this.pk(model)} FROM [${model}] ${sqlCondition ? `WHERE ${sqlCondition}` : ''}`)
            .then(list => {
                let ids = [];
                for (let i = list.length; i--;) {
                    ids.push(list[i][this.pk(model)]);
                }
                if (!ids.length) return [];
                return this.query(`DELETE FROM [${model}] WHERE ${this.pk(model)} IN (${ids.join(',')})`)
                    .then(deleteResult => {
                        return ids;
                    })
            })
            .then(ids => {
                result.items = ids;
                return result;
            })
            .catch(err => {
                result.error = new Err(Err.Code.DBDelete, err && err.message);
                return Promise.reject(result);
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
                    relations[key.toString()] = value[key]
                } else if (schemaFields[key].properties.type == FieldType.List) {
                    lists[key.toString()] = value[key]
                } else {
                    let thisValue: string | number = `${this.escape(value[key])}`;
                    properties.push({field: key, value: thisValue})
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
        query.offset = query.offset ? query.offset : (query.page ? query.page - 1 : 0 ) * query.limit;
        params.limit = '';
        let defaultSort = false;
        if (+query.limit) {
            params.limit = ` OFFSET ${query.offset} ROWS FETCH NEXT ${query.limit} ROWS ONLY `;
        }
        params.orderBy = '';
        if (query.orderBy.length) {
            let orderArray = [];
            for (let i = 0; i < query.orderBy.length; i++) {
                orderArray.push(`[${alias}].${query.orderBy[i].field} ${query.orderBy[i].ascending ? 'ASC' : 'DESC'}`);
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
                if (modelFields[query.fields[i].toString()] && modelFields[query.fields[i].toString()].properties.type == FieldType.List) continue;
                fields.push(`[${alias}].${query.fields[i]}`)
            }
        } else {
            for (let key in modelFields) {
                if (modelFields.hasOwnProperty(key)) {
                    if (modelFields[key].properties.type == FieldType.List) continue;
                    if (modelFields[key].properties.type != FieldType.Relation) {
                        fields.push(`[${alias}].${modelFields[key].fieldName}`);
                    } else if ((!query.relations || query.relations.indexOf(modelFields[key].fieldName) < 0) && modelFields[key].properties.relation.type != RelationType.Many2Many) {
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
                            if (relatedModelFields[filedNameList[j]].properties.type != FieldType.Relation || relatedModelFields[filedNameList[j]].properties.relation.type != RelationType.Many2Many) {
                                modelFiledList.push(`'"${filedNameList[j]}":','"',c.${filedNameList[j]},'"'`)
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
            params.condition = this.getCondition(alias, query.condition);
            params.condition = params.condition ? params.condition : '';
        }
        params.join = '';
        if (query.joins && query.joins.length) {
            let joins = [];
            for (let i = 0; i < query.joins.length; i++) {
                let join = query.joins[i];
                let type = '';
                switch (join.type) {
                    case Vql.Join :
                        type = 'FULL OUTER JOIN';
                        break;
                    case Vql.LeftJoin :
                        type = 'LEFT JOIN';
                        break;
                    case Vql.RightJoin :
                        type = 'RIGHT JOIN';
                        break;
                    case Vql.InnerJoin :
                        type = 'INNER JOIN';
                        break;
                    default :
                        type = 'LEFT JOIN';
                }
                let modelsAlias = join.vql.model + Math.floor(Math.random() * 100).toString();
                joins.push(`${type} [${join.vql.model}] as ${modelsAlias} ON ([${alias}].${join.field} = [${modelsAlias}].${this.pk(join.vql.model)})`);
                let joinParam = this.getQueryParams(join.vql, modelsAlias);
                if (joinParam.fields) {
                    fields.push(joinParam.fields);
                }
                if (joinParam.condition) {
                    params.condition = params.condition ? `(${params.condition} AND ${joinParam.condition})` : joinParam.condition
                }
                if (joinParam.orderBy) {
                    params.orderBy = params.orderBy && !defaultSort ? `${params.orderBy},${joinParam.orderBy}` : joinParam.orderBy;
                }
                if (joinParam.join) {
                    joins.push(joinParam.join)
                }
            }
            params.join = joins.join('\n');
        }
        params.fields = fields.join(',');
        return params;
    }

    private getCondition(model: string, condition: Condition) {
        model = condition.model || model;
        let operator = this.getOperatorSymbol(condition.operator);
        if (!condition.isConnector) {
            return `([${model}].${condition.comparison.field} ${operator} ${condition.comparison.isValueOfTypeField ? `[${model}].${condition.comparison.value}` : `${this.escape(condition.comparison.value)}`})`;
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

    private getManyToManyRelation(list: Array<any>, query: Vql) {
        let ids = [];
        let runRelatedQuery = (i) => {
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
            return this.query(`SELECT ${fields},r.${leftKey},r.${rightKey}  FROM [${relationship.model.schema.name}] m 
                LEFT JOIN [${query.model + 'Has' + this.pascalCase(relationName)}] r 
                ON (m.${this.pk(relationship.model.schema.name)} = r.${rightKey}) 
                WHERE r.${leftKey} IN (${ids.join(',')})`)
                .then(relatedList => {
                    let result = {};
                    result[relationName] = relatedList;
                    return result;
                })


        };
        for (let i = list.length; i--;) {
            ids.push(list[i][this.pk(query.model)]);
        }
        let relations: Array<Promise<any>> = [];
        let relationship;
        if (ids.length && query.relations && query.relations.length) {
            for (let i = query.relations.length; i--;) {
                let relationName = typeof query.relations[i] == 'string' ? query.relations[i] : query.relations[i]['name'];
                relationship = this.schemaList[query.model].getFields()[relationName].properties.relation;
                if (relationship.type == RelationType.Many2Many) {
                    relations.push(runRelatedQuery(i))
                }
            }
        }
        if (!relations.length) return Promise.resolve(list);
        return Promise.all(relations)
            .then(data => {
                let leftKey = this.camelCase(query.model);
                let rightKey = this.camelCase(relationship.model.schema.name);
                for (let i = data.length; i--;) {
                    for (let related in data[i]) {
                        if (data[i].hasOwnProperty(related)) {
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

    private getLists(list: Array<any>, query: Vql) {
        let runListQuery = (listName) => {
            let name = query.model + this.pascalCase(listName) + 'List';
            return this.query(`SELECT * FROM [${name}] WHERE [fk] IN (${ids.join(',')})`)
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
                    fields.hasOwnProperty(key) &&
                    fields[key].properties.type == FieldType.Relation &&
                    fields[key].properties.relation.type != RelationType.Many2Many) {
                    list[i][key] = this.parseJson(list[i][key]);
                }
            }
        }
        return list;
    }

    private parseJson(str) {
        if (typeof str == 'string' && str) {
            let replace = ['\\n', '\\b', '\\r', '\\t', '\\v', "\\'"];
            let search = ['\n', '\b', '\r', '\t', '\v', '\''];
            for (let i = search.length; i--;) {
                str = str.replace(search[i], replace[i]);
            }
            let json;
            try {
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
        let translateTablePromise = Promise.resolve(true);
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

    private qoute(str) {
        return `${str}`;
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
        if (properties.type == FieldType.List || (properties.relation && properties.relation.type == RelationType.Many2Many)) {
            return '';
        }
        let columnSyntax = `${filed.fieldName} ${this.getType(properties)}`;
        let defaultValue = properties.type != FieldType.Boolean ? `'${properties.default}'` : !!properties.default;
        columnSyntax += (properties.required && properties.type != FieldType.Relation) || properties.primary ? ' NOT NULL' : '';
        columnSyntax += properties.default ? ` DEFAULT '${defaultValue}'` : '';
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
                    typeSyntax = `NVARCHAR(${properties.maxLength ? properties.maxLength : 255 })`;
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
                typeSyntax = `BINARY`;
                break;
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

    private addOneToManyRelation<T, M>(model: T, relation: string, value: number
        | { [property: string]: any }): Promise<IUpsertResult<M>> {
        let result: IUpsertResult<T> = <IUpsertResult<T>>{};
        let modelName = model.constructor['schema'].name;
        let fields = this.schemaList[modelName].getFields();
        let relatedModelName = fields[relation].properties.relation.model.schema.name;
        let readIdPromise;
        if (fields[relation].properties.relation.isWeek && typeof value == 'object' && !value[this.pk(relatedModelName)]) {
            let relatedObject = new fields[relation].properties.relation.model(value);
            readIdPromise = relatedObject.insert().then(result => {
                return result.items[0][this.pk(relatedModelName)];
            })
        } else {
            let id;
            if (+value) {
                id = +value;
            } else if (typeof value == 'object') {
                id = +value[this.pk(relatedModelName)]
            }
            if (!id || id <= 0) return Promise.reject(new Error(`invalid <<${relation}>> related model id`));
            readIdPromise = Promise.resolve(id);
        }
        return readIdPromise
            .then(id => {
                return this.query<Array<T>>(`UPDATE [${modelName}] SET [${relation}] = '${id}' WHERE ${this.pk(relatedModelName)}='${model[this.pk(relatedModelName)]}' `)
            })
            .then(updateResult => {
                result.items = updateResult;
                return result;
            })
            .catch(err => {
                return Promise.reject(new Err(Err.Code.DBUpdate, err && err.message));
            })

    }


    private addManyToManyRelation<T, M>(model: T, relation: string, value: number
        | Array<number>
        | M
        | Array<M>): Promise<IUpsertResult<M>> {
        let result: IUpsertResult<T> = <IUpsertResult<T>>{};
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
                return this.insertAll(relatedModelName, newRelation)
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
                    insertList.push(`(${model[this.pk(modelName)]},${relationIds[i]})`);
                }
                return this.query<any>(`INSERT INTO ${modelName}Has${this.pascalCase(relation)}
                    ([${this.camelCase(modelName)}],[${this.camelCase(relatedModelName)}]) VALUES ${insertList.join(',')}`)
                    .then(insertResult => {
                        result.items = insertResult;
                        return result
                    })

            })
            .catch(err => {
                return Promise.reject(new Err(Err.Code.DBInsert, err && err.message));
            });

    }

    private removeOneToManyRelation<T>(model: T, relation: string) {
        let modelName = model.constructor['schema'].name;
        let result: IUpsertResult<T> = <IUpsertResult<T>>{};
        let relatedModelName = this.schemaList[modelName].getFields()[relation].properties.relation.model.schema.name;
        let isWeek = this.schemaList[modelName].getFields()[relation].properties.relation.isWeek;
        let preparePromise: Promise<number> = Promise.resolve(0);
        if (isWeek) {
            let readRelationId: Promise<number> = +model[relation] ? Promise.resolve(+model[relation]) : this.findById(modelName, model[this.pk(modelName)]).then(result => result.items[0][relation]);
            readRelationId.then(relationId => {
                return this.deleteOne(relatedModelName, relationId).then(() => relationId);
            })
        }
        return preparePromise
            .then(() => {
                return this.query<any>(`UPDATE [${model}] SET ${relation} = 0 WHERE ${this.pk(modelName)} = ${this.escape(model[this.pk(modelName)])}`)
                    .then(updateResult => {
                        result.items = updateResult;
                        return result;
                    })

            })
            .catch(err => {
                return Promise.reject(new Err(Err.Code.DBUpdate, err && err.message))
            })

    }

    private removeManyToManyRelation<T>(model: T, relation: string, condition: Condition): Promise<any> {
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
                return this.query<Array<any>>(`SELECT * FROM ${model + 'Has' + this.pascalCase(relation)} WHERE ${this.camelCase(modelName)} = ${model[this.pk(modelName)]} ${conditionsStr}`)
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
                return this.query(`DELETE FROM ${model + 'Has' + this.pascalCase(relation)} WHERE ${this.camelCase(modelName)} = ${model[this.pk(modelName)]} AND ${idCondition}}`)
                    .then(() => {
                        let result = {items: ids};
                        if (isWeek && ids.length) {
                            return this.deleteAll(relatedModelName, condition).then(() => result);
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

    public query<T>(query: string): Promise<T> {
        return new Promise((resolve, reject) => {
            this.connection.query(query, (err, result) => {
                if (err) return reject(err);
                resolve(<T>result);
            })
        })
    }

    public close(destroy: boolean): Promise<boolean> {
        return new Promise((resolve, reject) => {
            this.SQLConnection.close((err) => {
                if (err) reject(err);
                else resolve(true);
            });
        })
    }

}