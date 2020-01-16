import {repository, IsolationLevel} from '@loopback/repository';
import {post, getModelSchemaRef, requestBody} from '@loopback/rest';
import {Material} from '../models';
import {MaterialWithTxRepository} from '../repositories';
import {KafkaClientService} from '../services';
import {service} from '@loopback/core';

export class MaterialPostWithKafkaSubmitController {
  constructor(
    @repository(MaterialWithTxRepository)
    public materialWithTxRepository: MaterialWithTxRepository,
    @service(KafkaClientService)
    public kafkaClientServices: KafkaClientService,
  ) {}

  @post('/mms', {
    responses: {
      '200': {
        description: 'Material model instance',
        content: {'application/json': {schema: getModelSchemaRef(Material)}},
      },
    },
  })
  async create(
    @requestBody({
      content: {
        'application/json': {
          schema: getModelSchemaRef(Material, {
            title: 'NewMaterial',
            exclude: ['id'],
          }),
        },
      },
    })
    material: Omit<Material, 'id'>,
  ): Promise<Material> {
    console.log(
      `C: running post with incoming material: ${JSON.stringify(material)}`,
    );
    const {id, kmat, mvm, hmotnost, mnozstvi} = material;

    //vytvoreni transakce, timeout pro rollback 3sec
    const tx = await this.materialWithTxRepository.beginTransaction({
      isolationLevel: IsolationLevel.READ_COMMITTED,
      timeout: 3000,
    });

    //insert v ramci transakce
    const result1 = await this.materialWithTxRepository.create(material, {
      transaction: tx,
    });
    console.log(
      `C db update result: ${JSON.stringify(
        result1,
      )} -> going to commit/rollback`,
    );

    //Prace s Kafka je take externalizovano do sdilene sluzby
    try {
      const result2 = await this.kafkaClientServices.sendEventP(
        id,
        kmat,
        mvm,
        'test',
        hmotnost,
        mnozstvi,
      );
      console.log(
        `C kafka submit result: ${JSON.stringify(result2)} -> going to commit`,
      );
      await tx.commit();
      return result1;
    } catch (err) {
      console.log(
        `C kafka submit failure: ${JSON.stringify(err)} -> going to rollback`,
      );
      await tx.rollback();
    }

    return result1;
  }
}
