import { Module } from '@nestjs/common';
import { ChatMessageService } from './chat-message.service';
import { ChatController } from './chat.controller';
import { ChatService } from './chat.service';
import { GraphModule } from './graph/graph.module';

@Module({
  imports: [GraphModule],
  providers: [ChatService, ChatMessageService],
  controllers: [ChatController],
  exports: [ChatService],
})
export class ChatModule {}
