import { Module } from '@nestjs/common';
import { AiModule } from '../../ai/ai.module';
import { BaseSqlExecutorModule } from '../../base-sql-executor/base-sql-executor.module';
import { FieldModule } from '../../field/field.module';
import { RecordModule } from '../../record/record.module';
import { GraphService } from './graph.service';

@Module({
  imports: [FieldModule, RecordModule, BaseSqlExecutorModule, AiModule],
  providers: [GraphService],
  exports: [GraphService],
})
export class GraphModule {}
