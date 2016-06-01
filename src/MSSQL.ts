import {Connection, config, Request} from "mssql";
import {Schema} from "vesta-schema/Schema";
import {IDatabaseConfig, Database, IQueryOption, ISchemaList, IModelCollection} from "vesta-schema/Database";
import {Err} from "vesta-util/Err";
import {DatabaseError} from "vesta-schema/error/DatabaseError";
import {IDeleteResult, IUpsertResult, IQueryResult} from "vesta-schema/ICRUDResult";
import {Condition, Vql} from "vesta-schema/Vql";
import {FieldType, Relationship, Field, IFieldProperties} from "vesta-schema/Field";
import {IModelFields, Model} from "vesta-schema/Model";

interface ICalculatedQueryOptions {
    limit:string,
    orderBy:string,
    fields:string,
    condition:string,
}

export class MSSQL extends Database {
    private SQLConnection:Connection;
    private schemaList:ISchemaList = {};
    private config:IDatabaseConfig;
    private models:IModelCollection;

    get connection() {
        return new Request(this.SQLConnection);
    }

    public connect():Promise<Database> {
        if (this.SQLConnection) return Promise.resolve(this);
        return new Promise<Database>((resolve, reject)=> {
            this.SQLConnection = new Connection(<config>{
                server: this.config.host,
                port: +this.config.port,
                user: this.config.user,
                password: this.config.password,
                database: this.config.database,
                pool: {
                    min: 5,
                    max: 100,
                    idleTimeoutMillis: 1000,
                }
            });
            this.SQLConnection.connect((err)=> {
                if (err) {
                    return reject(new DatabaseError(Err.Code.DBConnection, err.message));
                }
                resolve(this);

            })
        })
    }

    constructor(config:IDatabaseConfig, models:IModelCollection) {
        super();
        var schemaList:ISchemaList = {};
        for (var model in models) {
            if (models.hasOwnProperty(model)) {
                schemaList[model] = models[model].schema;
            }
        }
        this.schemaList = schemaList;
        this.models = models;
        this.config = config;
    }

    public init():Promise<boolean> {
        var createSchemaPromise = this.initializeDatabase();
        for (var schema in this.schemaList) {
            if (this.schemaList.hasOwnProperty(schema)) {
                createSchemaPromise = createSchemaPromise.then(this.createTable(this.schemaList[schema]));
            }
        }
        return createSchemaPromise;
    }

    public findById<T>(model:string, id:number | string, option:IQueryOption = {}):Promise <IQueryResult<T>> {
        var query = new Vql(model);
        query.where(new Condition(Condition.Operator.EqualTo).compare('id', id));
        if (option.fields) query.select(...option.fields);
        if (option.relations) query.fetchRecordFor(...option.relations);
        query.orderBy = option.orderBy || [];
        query.limitTo(1);
        return this.findByQuery(query);
    }

    public findByModelValues<T>(model:string, modelValues:T, option:IQueryOption = {}):Promise < IQueryResult <T>> {
        var condition = new Condition(Condition.Operator.And);
        for (var key in modelValues) {
            if (modelValues.hasOwnProperty(key)) {
                condition.append((new Condition(Condition.Operator.EqualTo)).compare(key, modelValues[key]));
            }
        }
        var query = new Vql(model);
        if (option.fields) query.select(...option.fields);
        if (option.offset || option.page) query.fromOffset(option.offset ? option.offset : (option.page - 1) * option.limit);
        if (option.relations) query.fetchRecordFor(...option.relations);
        if (+option.limit) query.limitTo(option.limit);
        query.where(condition);
        query.orderBy = option.orderBy || [];
        return this.findByQuery(query);
    }

    public findByQuery<T>(query:Vql):Promise < IQueryResult <T>> {
        var params:ICalculatedQueryOptions = this.getQueryParams(query);
        var result:IQueryResult<T> = <IQueryResult<T>>{};
        var totalPromise = this.query(`SELECT COUNT(*) AS [total] FROM ${query.model} ${params.condition}`);
        var itemsPromise = this.query(`SELECT ${params.limit} ${params.fields} FROM ${query.model} ${params.condition} ${params.orderBy}`);
        return Promise.all([totalPromise,itemsPromise])
            .then(data=> {
                var list = data[1];
                result.total = data[0][0]['total'];
                return this.getManyToManyRelation(<Array<T>>list, query)
                    .then(list=> {
                        result.items = this.normalizeList(this.schemaList[query.model], list);
                        return result;
            });
        })
            .catch(err=> {
                if (err) {
                    result.error = new Err(Err.Code.DBQuery);
                    return Promise.reject(result);
                }
            })
    }

    public insertOne<T>(model:string, value:T):Promise < IUpsertResult <T>> {
        var result:IUpsertResult<T> = <IUpsertResult<T>>{};
        var analysedValue = this.getAnalysedValue<T>(model, value);
        var fields = [];
        var values = [];
        for (var i = analysedValue.properties.length; i--;) {
            fields.push(analysedValue.properties[i].field);
            values.push(analysedValue.properties[i].value);
        }

        return this.query(`INSERT INTO ${model} (${fields.join(',')}) VALUES (${values.join(',')});SELECT SCOPE_IDENTITY() as id;`)
            .then(insertResult=> {
                var steps = [];
                var id = insertResult[0]['id'];
                for (var key in analysedValue.relations) {
                    if (analysedValue.relations.hasOwnProperty(key)) {
                        steps.push(this.addRelation(new this.models[model]({id: insertResult['insertId']}), key, analysedValue.relations[key]));
                    }

                }
                return Promise.all(steps).then(()=>this.query(`SELECT * FROM ${model} WHERE id = ${id}`));
            })
            .then(list=> {
                result.items = <Array<T>>list;
                return result;
            })
            .catch(err=> {
                result.error = new Err(Err.Code.DBInsert, err.message);
                return Promise.reject(result);
        });
    }

    public insertAll<T>(model:string, value:Array<T>):Promise < IUpsertResult <T>> {
        var result:IUpsertResult<T> = <IUpsertResult<T>>{};
        var fields = this.schemaList[model].getFields();
        var fieldsName = [];
        var insertList = [];
        for (var field in fields) {
            if (fields.hasOwnProperty(field) && fields[field].properties.type != FieldType.Relation || fields[field].properties.relation.type != Relationship.Type.Many2Many) {
                fieldsName.push(field);
            }
        }
        for (var i = value.length; i--;) {
            var insertPart = [];
            for (var j = fieldsName.length; j--;) {
                insertPart.push(value[i].hasOwnProperty(fieldsName[j]) ? `'${value[i][fieldsName[j]]}'` : '\'\'');
            }
            insertList.push(`(${insertPart.join(',')})`)

        }

        return this.query<Array<T>>(`INSERT INTO ${model}} (${fieldsName.join(',')}) VALUES ${insertList.join(',')}`)
            .then(insertResult=> {
                result.items = insertResult;
                return result;
        })
            .catch(err=> {
                result.error = new Err(Err.Code.DBInsert, err.message);
                return Promise.reject(result)
            });

    }

    public addRelation<T,M>(model:T, relation:string, value:number|Array<number>|M|Array<M>):Promise<IUpsertResult<M>> {
        var modelName = model.constructor['schema'].name;
        var fields = this.schemaList[modelName].getFields();
        if (fields[relation] && fields[relation].properties.type == FieldType.Relation) {
            if (fields[relation].properties.relation.type != Relationship.Type.Many2Many) {
                return this.addOneToManyRelation<T,M>(model, relation, value)
            } else {
                return this.addManyToManyRelation<T,M>(model, relation, value)
            }
        }
        return Promise.reject(new Err(Err.Code.DBInsert));
    }

    public removeRelation<T>(model:string, relation:string, condition:Condition|number) {
        var safeCondition:Condition;
        if (typeof condition == 'number') {
            safeCondition = new Condition(Condition.Operator.EqualTo);
            safeCondition.compare('id', condition);
        } else {
            safeCondition = <Condition>condition;
        }
        var modelName = model.constructor['schema'].name;
        var fields = this.schemaList[modelName].getFields();
        if (fields[relation] && fields[relation].properties.type == FieldType.Relation) {
            if (fields[relation].properties.relation.type != Relationship.Type.Many2Many) {
                return this.removeOneToManyRelation(model, relation, safeCondition)
            } else {
                return this.removeManyToManyRelation(model, relation, safeCondition)
            }
        }
        return Promise.reject(new Err(Err.Code.DBDelete));
    }

    private updateRelations(model:Model, relation, relatedValues) {
        var modelName = model.constructor['schema'].name;
        var ids = [0];
        if (relatedValues instanceof Array) {
            for (var i = relatedValues.length; i--;) {
                ids.push(typeof relatedValues[i] == 'object' ? relatedValues[i].id : relatedValues[i]);
            }
        }
        return this.query(`DELETE FROM ${this.pascalCase(modelName)}Has${this.pascalCase(relation)} 
                    WHERE ${this.camelCase(modelName)} = ${model['id']}`)
            .then(()=> {
                return this.addRelation(model, relation, ids)
            })
    }

    public updateOne<T>(model:string, value:T):Promise < IUpsertResult <T>> {
        var result:IUpsertResult<T> = <IUpsertResult<T>>{};
        var analysedValue = this.getAnalysedValue<T>(model, value);
        var properties = [];
        for (var i = analysedValue.properties.length; i--;) {
            properties.push(`${analysedValue.properties[i].field} = ${analysedValue.properties[i].value}`);
        }
        var id = value['id'];
        var steps = [];
        for (var relation in analysedValue.relations) {
            if (analysedValue.relations.hasOwnProperty(relation)) {
                if (this.schemaList[model].getFields()[relation].properties.relation.type != Relationship.Type.Many2Many) {
                    properties.push(`${relation} = ${this.escape(analysedValue.relations[relation])}`);
                } else {
                    steps.push(this.updateRelations(new this.models[model](value), relation, analysedValue.relations[relation]));
                }
            }
        }

        return Promise.all(steps)
            .then(()=>this.query<Array<T>>(`UPDATE ${model} SET ${properties.join(',')} WHERE id = ${value['id']}`))
            .then(()=>this.findById(model, value['id']))
            .catch(err=> {
                result.error = new Err(Err.Code.DBQuery, err.message);
                return Promise.reject(result);
        });

    }

    public updateAll<T>(model:string, newValues:T, condition:Condition):Promise < IUpsertResult < T >> {
        var sqlCondition = this.getCondition(condition);
        var result:IUpsertResult<T> = <IUpsertResult<T>>{};
        var properties = [];
        for (var key in newValues) {
            if (newValues.hasOwnProperty(key) && this.schemaList[model].getFieldsNames().indexOf(key) >= 0 && key != 'id') {
                properties.push(`${model}.${key} = '${newValues[key]}'`)
            }
        }
        return this.query<Array<T>>(`SELECT id FROM ${model} ${sqlCondition ? `WHERE ${sqlCondition}` : ''}`)
            .then(list=> {
                var ids = [];
                for (var i = list.length; i--;) {
                    ids.push(list[i]['id']);
                            }
                if (!ids.length) return [];
                return this.query<any>(`UPDATE ${model} SET ${properties.join(',')}  WHERE id IN (${ids.join(',')})}`)
                    .then(updateResult=> {
                        return this.query<Array<T>>(`SELECT * FROM ${model} WHERE id IN (${ids.join(',')})`)
                    })
            })
            .then(list=> {
                            result.items = list;
                return result
            })
            .catch(err=> {
                result.error = new Err(Err.Code.DBUpdate, err.message);
                return Promise.reject(result);
        });
    }

    public deleteOne(model:string, id:number | string):Promise < IDeleteResult > {
        var result:IDeleteResult = <IDeleteResult>{};
        var fields = this.schemaList[model].getFields();
        return this.query(`DELETE FROM ${model} WHERE id = ${id}`)
            .then(deleteResult=> {
                for (var field in this.schemaList[model].getFields()) {
                    if (fields.hasOwnProperty(field) && fields[field].properties.type == FieldType.Relation) {
                        this.removeRelation(model, field, 0)
                    }
                }
                result.items = [id];
                return result;
            })
            .catch(err=> {
                result.error = new Err(Err.Code.DBDelete);
                return Promise.reject(result);
            })
    }

    public deleteAll<T>(model:string, condition:Condition):Promise < IDeleteResult > {
        var sqlCondition = this.getCondition(condition);
        var result:IDeleteResult = <IDeleteResult>{};
        return this.query<Array<T>>(`SELECT id FROM ${model} ${sqlCondition ? `WHERE ${sqlCondition}` : ''}`)
            .then(list=> {
                var ids = [];
                for (var i = list.length; i--;) {
                    ids.push(list[i]['id']);
                        }
                if (!ids.length) return [];
                return this.query(`DELETE FROM ${model} WHERE id IN (${ids.join(',')})`)
                    .then(deleteResult=> {
                        return ids;
                    })
            })
            .then(ids=> {
                        result.items = ids;
                return result;
            })
            .catch(err=> {
                result.error = new Err(Err.Code.DBDelete, err.message);
                return Promise.reject(result);
            })
    }

    private getAnalysedValue<T>(model:string, value:T) {
        var properties = [];
        var schemaFieldsName = this.schemaList[model].getFieldsNames();
        var schemaFields = this.schemaList[model].getFields();
        var relations = {};
        for (var key in value) {
            if (value.hasOwnProperty(key) && schemaFieldsName.indexOf(key) >= 0 && value[key] !== undefined) {
                if (schemaFields[key].properties.type != FieldType.Relation) {
                    var thisValue:string|number = `N${this.escape(value[key])}`;
                    if (FieldType.Boolean == schemaFields[key].properties.type) {
                        thisValue = value[key] ? 1 : 0
                    }
                    properties.push({field: key, value: thisValue})
                } else {
                    relations[key] = value[key]
                }

            }
        }
        return {
            properties: properties,
            relations: relations,
        }
    }

    private getQueryParams(query:Vql):ICalculatedQueryOptions {
        var params:ICalculatedQueryOptions = <ICalculatedQueryOptions>{};
        query.offset = query.offset ? query.offset : (query.page ? query.page - 1 : 0 ) * query.limit;
        params.limit = '';
        if (+query.limit) {
            params.limit = `TOP ${query.limit}`;
        }
        params.orderBy = '';
        if (query.orderBy.length) {
            var orderArray = [];
            for (var i = 0; i < query.orderBy.length; i--) {
                orderArray.push(`${query.model}.${query.orderBy[i].field} ${query.orderBy[i].ascending ? 'ASC' : 'DESC'}`);
            }
            params.orderBy = `ORDER BY ${orderArray.join(',')}`;
        }
        var fields:Array<string> = [];
        var modelFields = this.schemaList[query.model].getFields();
        if (query.fields && query.fields.length) {
            for (var i = 0; i < query.fields.length; i++) {
                fields.push(`${query.model}.${query.fields[i]}`)
            }
        } else {
            for (var key in modelFields) {
                if (modelFields.hasOwnProperty(key)) {
                    if (modelFields[key].properties.type != FieldType.Relation) {
                        fields.push(`${query.model}.${modelFields[key].fieldName}`);
                    } else if ((!query.relations || query.relations.indexOf(modelFields[key].fieldName) < 0) && modelFields[key].properties.relation.type != Relationship.Type.Many2Many) {
                        fields.push(`${query.model}.${modelFields[key].fieldName}`);
                    }
                }
            }
        }

        for (var i = 0; i < query.relations.length; i++) {
            var relationName:string = typeof query.relations[i] == 'string' ? query.relations[i] : query.relations[i]['name'];
            var field:Field = modelFields[relationName];
            if (!field) {
                throw `FIELD ${relationName} NOT FOUND IN model ${query.model}`
            }
            var properties = field.properties;
            if (properties.type == FieldType.Relation) {
                if (properties.relation.type == Relationship.Type.One2Many || properties.relation.type == Relationship.Type.One2One) {
                    var relatedModelName = properties.relation.model.schema.name;
                    var modelFiledList = [];
                    var filedNameList = properties.relation.model.schema.getFieldsNames();
                    var relatedModelFields = properties.relation.model.schema.getFields();
                    for (var j = 0; j < filedNameList.length; j++) {

                        if (typeof query.relations[i] == 'string' || query.relations[i]['fields'].indexOf(filedNameList[j]) >= 0) {
                            if (relatedModelFields[filedNameList[j]].properties.type != FieldType.Relation || relatedModelFields[filedNameList[j]].properties.relation.type != Relationship.Type.Many2Many) {
                                modelFiledList.push(`'"${filedNameList[j]}":','"',${this.qoute(filedNameList[j])},'"'`)
                            }
                        }
                    }
                    modelFiledList.length && fields.push(`(SELECT CONCAT('{',${modelFiledList.join(',",",')},'}') FROM ${properties.relation.model.schema.name} WHERE ${relatedModelName}.id = ${query.model}.${field.fieldName}  LIMIT 1) as ${field.fieldName}`)
                }
            }
        }
        params.fields = fields.join(',');
        params.condition = '';
        if (query.condition) {
            params.condition = this.getCondition(query.condition);
            params.condition = params.condition ? `WHERE ${params.condition}` : '';
        }
        return params;
    }

    private getCondition(condition:Condition) {
        var operator = this.getOperatorSymbol(condition.operator);
        if (!condition.isConnector) {
            return `(${condition.comparison.field} ${operator} ${condition.comparison.isValueOfTypeField ? condition.comparison.value : `${this.escape(condition.comparison.value)}`})`;
        } else {
            var childrenCondition = [];
            for (var i = 0; i < condition.children.length; i++) {
                childrenCondition.push(this.getCondition(condition.children[i]));
            }
            return `(${childrenCondition.join(` ${operator} `)})`;
        }
    }

    private getManyToManyRelation(list:Array < any >, query:Vql) {
        var ids = [];
        var runRelatedQuery = (i)=> {
            var relationName = typeof query.relations[i] == 'string' ? query.relations[i] : query.relations[i]['name'];
            var relationship = this.schemaList[query.model].getFields()[relationName].properties.relation;
            var fields = '*';
            if (typeof query.relations[i] != 'string') {
                for (var j = query.relations[i]['fields'].length; j--;) {
                    query.relations[i]['fields'][j] = `${query.relations[i]['fields'][j]}`;
                }
                fields = query.relations[i]['fields'].join(',');
            }
                var leftKey = this.camelCase(query.model);
                var rightKey = this.camelCase(relationship.model.schema.name);
            return this.query(`SELECT ${fields},r.${leftKey},r.${rightKey}  FROM ${relationship.model.schema.name} m 
                LEFT JOIN ${query.model + 'Has' + this.pascalCase(relationName)} r 
                ON (m.id = r.${rightKey}) 
                WHERE r.${leftKey} IN (${ids.join(',')})`)
                .then(relatedList=> {
                    var result = {};
                    result[relationName] = relatedList;
                    return result;
                })


        };
        for (var i = list.length; i--;) {
            ids.push(list[i]['id']);
        }
        var relations:Array<Promise<any>> = [];
        if (ids.length && query.relations && query.relations.length) {
            for (var i = query.relations.length; i--;) {
                var relationName = typeof query.relations[i] == 'string' ? query.relations[i] : query.relations[i]['name'];
                var relationship = this.schemaList[query.model].getFields()[relationName].properties.relation;
                if (relationship.type == Relationship.Type.Many2Many) {
                    relations.push(runRelatedQuery(i))
                }
            }
        }
        if (!relations.length) return Promise.resolve(list);
        return Promise.all(relations)
            .then(data=> {
                var leftKey = this.camelCase(query.model);
                var rightKey = this.camelCase(relationship.model.schema.name);
                for (var i = data.length; i--;) {
                    for (var related in data[i]) {
                        if (data[i].hasOwnProperty(related)) {
                            for (var k = list.length; k--;) {
                                var id = list[k]['id'];
                                list[k][related] = [];
                                for (var j = data[i][related].length; j--;) {
                                    if (id == data[i][related][j][this.camelCase(query.model)]) {
                                        var relatedData = data[i][related][j];
                                        relatedData['id'] = relatedData[rightKey];
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

    private normalizeList(schema:Schema, list:Array < any >) {
        var fields:IModelFields = schema.getFields();
        for (var i = list.length; i--;) {
            for (var key in list[i]) {
                if (list[i].hasOwnProperty(key) &&
                    fields.hasOwnProperty(key) &&
                    fields[key].properties.type == FieldType.Relation &&
                    fields[key].properties.relation.type != Relationship.Type.Many2Many) {
                    list[i][key] = this.parseJson(list[i][key]);
                }
            }
        }
        return list;
    }

    private parseJson(str) {
        var search = ['\n', '\b', '\r', '\t', '\v', '\''];
        var replace = ['\\n', '\\b', '\\r', '\\t', '\\v', "\\'"];
        for (var i = search.length; i--;) {
            str = str.replace(search[i], replace[i]);
        }
        return JSON.parse(str);

    }

    private createTable(schema:Schema) {
        var fields = schema.getFields();
        var createDefinition = this.createDefinition(fields, schema.name);
        var ownTablePromise =
            this.query(`IF OBJECT_ID('${schema.name}', 'U') IS NOT NULL DROP TABLE ${schema.name}`)
                .then(()=> {
                    return this.query(`CREATE TABLE ${schema.name} (\n${createDefinition.ownColumn})\n`)
            });
        var translateTablePromise = Promise.resolve(true);
        if (createDefinition.lingualColumn) {
            translateTablePromise =
                this.query(`IF OBJECT_ID('${schema.name}_translation', 'U') DROP TABLE ${schema.name}_translation`)
                    .then(()=> {
                        return this.query(`CREATE TABLE ${schema.name}_translation (\n${createDefinition.lingualColumn}\n)`)
        });
                }


        return ()=> Promise.all([ownTablePromise, translateTablePromise].concat(createDefinition.relations));

    }

    private relationTable(field:Field, table:string):Promise < any > {
        var schema = new Schema(table + 'Has' + this.pascalCase(field.fieldName));
        schema.addField('id').primary().required();
        schema.addField(this.camelCase(table)).type(FieldType.Integer).required();
        schema.addField(this.camelCase(field.properties.relation.model.schema.name)).type(FieldType.Integer).required();
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

    private createDefinition(fields:IModelFields, table:string, checkMultiLingual = true) {
        var multiLingualDefinition:Array<String> = [];
        var columnDefinition:Array<String> = [];
        var relations:Array<Promise<boolean>> = [];
        var keyIndex;
        for (var field in fields) {
            if (fields.hasOwnProperty(field)) {
                keyIndex = fields[field].properties.primary ? field : keyIndex;
                var column = this.columnDefinition(fields[field]);
                if (column) {
                    if (fields[field].properties.multilingual && checkMultiLingual) {
                        multiLingualDefinition.push(column);
                    } else {
                        columnDefinition.push(column);
                    }
                } else if (fields[field].properties.type == FieldType.Relation && fields[field].properties.relation.type == Relationship.Type.Many2Many) {
                    relations.push(this.relationTable(fields[field], table));
                }
            }
        }
        var keyFiled;

        if (keyIndex) {
            keyFiled = fields[keyIndex];
        } else {
            keyFiled = new Field('id');
            keyFiled.primary().type(FieldType.Integer).required();
            columnDefinition.push(this.columnDefinition(keyFiled));
        }

        var keySyntax = `PRIMARY KEY (${keyFiled.fieldName})`;
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

    private columnDefinition(filed:Field) {
        var properties = filed.properties;
        if (properties.relation && properties.relation.type == Relationship.Type.Many2Many) {
            return '';
        }
        var columnSyntax = `${filed.fieldName} ${this.getType(properties)}`;
        columnSyntax += properties.required || properties.primary ? ' NOT NULL' : '';
        columnSyntax += properties.default ? ` DEFAULT '${properties.default}'` : '';
        columnSyntax += properties.unique ? ' UNIQUE ' : '';
        columnSyntax += properties.primary ? ' IDENTITY(1,1) ' : '';
        return columnSyntax;
    }

    private getType(properties:IFieldProperties) {
        var typeSyntax;
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
                if (properties.relation.type == Relationship.Type.One2One || properties.relation.type == Relationship.Type.One2Many) {
                    typeSyntax = 'BIGINT';
                }
                break;

        }
        return typeSyntax;
    }

    private initializeDatabase() {
        return Promise.resolve();
        // return this.query(`ALTER DATABASE ${this.config.database}  CHARSET = utf8 COLLATE = utf8_general_ci;`);
    }

    private getOperatorSymbol(operator:number):string {
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

    private addOneToManyRelation<T,M>(model:T, relation:string, value:number|{[property:string]:any}):Promise<IUpsertResult<M>> {
        var result:IUpsertResult<T> = <IUpsertResult<T>>{};
        var modelName = model.constructor['schema'].name;
        var fields = this.schemaList[modelName].getFields();
        if (fields[relation] && fields[relation].properties.type == FieldType.Relation
            && fields[relation].properties.relation.type != Relationship.Type.Many2Many) {
            var readIdPromise = Promise.reject(new Err(Err.Code.DBUpdate));
            if (fields[relation].properties.relation.isWeek && typeof value == 'object' && !value['id']) {
                var relatedObject = new fields[relation].properties.relation.model(value);
                readIdPromise = relatedObject.insert().then(result=> {
                    return result.items[0]['id'];
                })
            } else if (+value > 0) {
                var id = +value ? +value : +value['id'];
                readIdPromise = Promise.resolve(id);
            }
            return readIdPromise
                .then(id=> {
                    return this.query<Array<T>>(`UPDATE ${modelName} SET ${relation} = '${id}' WHERE id='${model['id']}' `)
                    })
                .then(updateResult=> {
                    result.items = updateResult;
                    return result;
                })
                .catch(err=> {
                    return Promise.reject(new Err(Err.Code.DBUpdate, err.message));
            })

        }
    }

    private addManyToManyRelation<T,M>(model:T, relation:string, value:number|Array<number>|M|Array<M>):Promise<IUpsertResult<M>> {
        var result:IUpsertResult<T> = <IUpsertResult<T>>{};
        var modelName = model.constructor['schema'].name;
        var fields = this.schemaList[modelName].getFields();
        var relatedModelName = fields[relation].properties.relation.model.schema.name;
        var newRelation = [];
        var relationIds = [];
        if (fields[relation] && fields[relation].properties.type == FieldType.Relation
            && fields[relation].properties.relation.type == Relationship.Type.Many2Many) {
            if (+value > 0) {
                relationIds.push(+value);
            } else if (value instanceof Array) {
                for (var i = value['length']; i--;) {
                    if (+value[i]) {
                        relationIds.push(+value[i])
                    } else if (typeof value[i] == 'object') {
                        if (+value[i]['id'])relationIds.push(+value[i]['id']);
                        else if (fields[relation].properties.relation.isWeek) newRelation.push(value[i])
                    }
                }
            } else if (typeof value == 'object') {
                if (+value['id']) {
                    relationIds.push(+value['id'])
                } else if (fields[relation].properties.relation.isWeek) newRelation.push(value)
            }
        } else {
            return Promise.reject(new Err(Err.Code.DBInsert));
        }
        return new Promise<Array<number>>(
            (resolve, reject)=> {
                if (!newRelation.length) {
                    return resolve(relationIds);
                }
                this.insertAll(relatedModelName, newRelation)
                    .then(result=> {
                        for (var i = result.items.length; i--;) {
                            relationIds.push(result.items[i].id);
                        }
                        resolve(relationIds)
                    })
                    .catch(()=> {
                        return reject(new Err(Err.Code.DBInsert));
                    })
            })
            .then(relationIds=> {
                var insertList = [];
                for (var i = relationIds.length; i--;) {
                    insertList.push(`(${model['id']},${relationIds[i]})`);
                }
                return this.query<any>(`INSERT INTO ${modelName}Has${this.pascalCase(relation)} 
                    (${this.camelCase(modelName)},${this.camelCase(relatedModelName)}) VALUES ${insertList.join(',')}`)
                    .then(insertResult=>{
                        result.items = insertResult;
                        return result
                    })
                    .catch(err=>{
                        return Promise.reject(new Err(Err.Code.DBInsert,err.message));
                })
            });

    }

    private removeOneToManyRelation<T>(model:string, relation:string, condition:Condition) {
        var paredCondition = this.getCondition(condition);
        var result:IUpsertResult<T> = <IUpsertResult<T>>{};
        var relatedModelName = this.schemaList[model].getFields()[relation].properties.relation.model.schema.name;
        var vql = new Vql(model);
        vql.select('id').where(condition);
        this.findByQuery(vql)
            .then(result=> {
                var deleteVql = new Vql(relatedModelName).select('id').where(new Condition(Condition.Operator.Or));
                for (var i = result.items.length; i--;) {
                    deleteVql.condition.append(new Condition(Condition.Operator.EqualTo).compare('id', result.items[i][relation]))
                }
                return deleteVql;
            })
            .then(deleteVql=> {
                return this.findByQuery(deleteVql)
                    .then(result=> {
                        var condition = new Condition(Condition.Operator.Or);
                        for (var i = result.items.length; i--;) {
                            condition.append(new Condition(Condition.Operator.EqualTo).compare('id', result.items[i]['id']))
                        }
                        return this.deleteAll(relatedModelName, condition);
                    })
            });
        return this.query<any>(`UPDATE ${model} SET ${relation} = 0 WHERE ${paredCondition}`)
            .then(updateResult=>{
                    result.items = updateResult;
                return result;
            })
            .catch(err=>{
                return Promise.reject(new Err(Err.Code.DBUpdate,err.message))
        })
    }

    private removeManyToManyRelation<T>(model:string, relation:string, condition:Condition) {
        var relatedModelName = this.schemaList[model].getFields()[relation].properties.relation.model.schema.name;
        var vql = new Vql(model);
        vql.select('id').where(condition);
        return this.findByQuery(vql)
            .then(result=> {
                var intermediateModel = model + 'Has' + this.pascalCase(relation);
                var relationCondition = new Condition(Condition.Operator.Or);
                for (var i = result.items.length; i--;) {
                    relationCondition.append(new Condition(Condition.Operator.EqualTo).compare(this.camelCase(model), result.items[i]['id']))
                }
                if (!this.schemaList[model].getFields()[relation].properties.relation.isWeek) return relationCondition;
                var relationVql = new Vql(intermediateModel).select(this.camelCase(relatedModelName)).where(relationCondition);
                return this.findByQuery(relationVql)
                    .then(relatedResult=> {
                        var condition = new Condition(Condition.Operator.Or);
                        for (var i = result.items.length; i--;) {
                            condition.append(new Condition(Condition.Operator.EqualTo).compare('id', relatedResult.items[i][this.camelCase(relatedModelName)]))
                        }
                        this.deleteAll(relatedModelName, condition);
                        return relationCondition
                    });
            })
            .then(relationCondition=> {
                var intermediateModel = model + 'Has' + this.pascalCase(relation);
                return this.deleteAll(intermediateModel, relationCondition)
            });

    }

    public escape(value) {
        return `'${value.replace('\'','\'\'')}'`;
    }

    private query<T>(query:string):Promise<T> {
        return new Promise((resolve, reject)=> {
            this.connection.query(query, (err, result)=> {
                if (err) return reject(err);
                resolve(<T>result);
            })
        })
    }

}