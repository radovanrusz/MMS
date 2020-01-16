import {DefaultTransactionalRepository} from '@loopback/repository';
import {Material, MaterialRelations} from '../models';
import {Db2DataSource} from '../datasources';
import {inject} from '@loopback/core';

export class MaterialWithTxRepository extends DefaultTransactionalRepository<
  Material,
  typeof Material.prototype.id,
  MaterialRelations
> {
  constructor(
    @inject('datasources.db2') dataSource: Db2DataSource,
  ) {
    super(Material, dataSource);
  }
}
