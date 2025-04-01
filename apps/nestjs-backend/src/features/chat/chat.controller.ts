/* eslint-disable sonarjs/no-duplicate-string */
import { Body, Controller, Delete, Get, HttpCode, Param, Post, Put, Res } from '@nestjs/common';
import type {
  IChatEventMessage,
  ICreateChatConversationVo,
  IGetChatConversationsVo,
  IGetChatConversationVo,
} from '@teable/openapi';
import {
  ICreateChatConversationRo,
  renameChatConversationRoSchema,
  IRenameChatConversationRo,
  chatCompletionsRoSchema,
  IChatCompletionsRo,
  ChatMessageType,
  createChatConversationRoSchema,
} from '@teable/openapi';
import { Response } from 'express';
import { ZodValidationPipe } from '../../zod.validation.pipe';
import { Permissions } from '../auth/decorators/permissions.decorator';
import { ChatMessageService } from './chat-message.service';
import { ChatService } from './chat.service';

@Controller('api/base/:baseId/chat')
@Permissions('base|read')
export class ChatController {
  constructor(
    private readonly chartService: ChatService,
    private readonly chatMessageService: ChatMessageService
  ) {}

  @Post('conversation')
  async createChatConversation(
    @Param('baseId') baseId: string,
    @Body(new ZodValidationPipe(createChatConversationRoSchema)) body: ICreateChatConversationRo
  ): Promise<ICreateChatConversationVo> {
    return this.chatMessageService.createChatConversation(baseId, body.question);
  }

  @Get('conversation/:sessionId')
  async getChatConversation(
    @Param('baseId') baseId: string,
    @Param('sessionId') sessionId: string
  ): Promise<IGetChatConversationVo> {
    return this.chatMessageService.getChatConversation(baseId, sessionId);
  }

  @Delete('conversation/:sessionId')
  async deleteChatConversation(
    @Param('baseId') baseId: string,
    @Param('sessionId') sessionId: string
  ) {
    return this.chatMessageService.deleteChatConversation(baseId, sessionId);
  }

  @Put('conversation/:sessionId')
  async renameChatConversation(
    @Param('baseId') baseId: string,
    @Param('sessionId') sessionId: string,
    @Body(new ZodValidationPipe(renameChatConversationRoSchema)) body: IRenameChatConversationRo
  ) {
    return this.chatMessageService.renameChatConversation(baseId, sessionId, body.name);
  }

  @Get('conversations')
  async getChatConversations(@Param('baseId') baseId: string): Promise<IGetChatConversationsVo> {
    return this.chatMessageService.getChatConversations(baseId);
  }

  @Post('completions/:sessionId')
  @HttpCode(200)
  completions(
    @Param('sessionId') sessionId: string,
    @Param('baseId') baseId: string,
    @Body(new ZodValidationPipe(chatCompletionsRoSchema)) body: IChatCompletionsRo,
    @Res() response: Response
  ) {
    response.setHeader('Content-Type', 'text/event-stream');
    response.setHeader('Cache-Control', 'no-cache');
    response.setHeader('Connection', 'keep-alive');
    const messageHandler = (chatEventMessage: IChatEventMessage) => {
      response.write(
        `event: ${chatEventMessage.event}\ndata: ${JSON.stringify(chatEventMessage.data)}\n\n`
      );
      if (chatEventMessage.event === ChatMessageType.Finish) {
        response.end();
      }
    };
    const abortController = new AbortController();
    response.on('close', () => {
      abortController.abort();
    });
    this.chartService
      .chat(sessionId, body, {
        messageHandler,
        signal: abortController.signal,
        baseId,
      })
      .finally(() => {
        response.end();
      });
  }
}
