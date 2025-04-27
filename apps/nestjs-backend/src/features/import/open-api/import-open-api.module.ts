import { Module } from '@nestjs/common';
import { ShareDbModule } from '../../../share-db/share-db.module';
import { NotificationModule } from '../../notification/notification.module';
import { RecordOpenApiModule } from '../../record/open-api/record-open-api.module';
import { TableOpenApiModule } from '../../table/open-api/table-open-api.module';
import { ImportCsvModule } from './import-csv.module';
import { ImportController } from './import-open-api.controller';
import { ImportOpenApiService } from './import-open-api.service';

@Module({
  imports: [
    TableOpenApiModule,
    RecordOpenApiModule,
    NotificationModule,
    ShareDbModule,
    ImportCsvModule,
  ],
  controllers: [ImportController],
  providers: [ImportOpenApiService],
  exports: [ImportOpenApiService],
})
export class ImportOpenApiModule {}
