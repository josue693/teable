import { Injectable, NotFoundException } from '@nestjs/common';
import { generateChatMessageId, generateChatSessionId } from '@teable/core';
import { PrismaService } from '@teable/db-main-prisma';
import { ChatMessageDataType, ChatMessageRole } from '@teable/openapi';
import type {
  IAiChatMessageContent,
  IGetChatConversationsVo,
  IGetChatConversationVo,
} from '@teable/openapi';
import { ClsService } from 'nestjs-cls';
import type { IClsStore } from '../../types/cls';

@Injectable()
export class ChatMessageService {
  constructor(
    private readonly prismaService: PrismaService,
    private readonly cls: ClsService<IClsStore>
  ) {}

  async createChatConversation(baseId: string, question?: string) {
    const name = question && question.length > 20 ? question.slice(0, 20) + '...' : '';
    const session = await this.prismaService.chatSession.create({
      data: { id: generateChatSessionId(), baseId, name, createdBy: this.cls.get('user.id') },
      select: {
        id: true,
        name: true,
      },
    });
    return {
      sessionId: session.id,
      name: session.name,
    };
  }

  async getChatConversations(baseId: string): Promise<IGetChatConversationsVo> {
    const res = await this.prismaService.chatSession.findMany({
      where: { baseId },
      select: {
        id: true,
        name: true,
        createdTime: true,
        createdBy: true,
      },
      orderBy: {
        createdTime: 'desc',
      },
    });
    return {
      conversations: res.map((item) => ({
        sessionId: item.id,
        name: item.name,
        createdTime: item.createdTime.toISOString(),
      })),
    };
  }

  async getChatConversation(_baseId: string, sessionId: string): Promise<IGetChatConversationVo> {
    const messages = await this.prismaService.chatMessage.findMany({
      where: { sessionId },
      orderBy: {
        createdTime: 'asc',
      },
      select: {
        id: true,
        role: true,
        content: true,
        createdTime: true,
        createdBy: true,
      },
    });

    return {
      messages: messages.map((item) => ({
        messageId: item.id,
        createdTime: item.createdTime.toISOString(),
        userId: item.createdBy,
        sessionId,
        ...(item.role === ChatMessageRole.Assistant
          ? {
              data: JSON.parse(item.content) as IAiChatMessageContent[],
              role: item.role as ChatMessageRole.Assistant,
            }
          : { data: item.content, role: item.role as ChatMessageRole.Human }),
      })),
    };
  }

  async deleteChatConversation(baseId: string, sessionId: string) {
    await this.prismaService.chatSession.deleteMany({
      where: { baseId, id: sessionId },
    });
  }

  async renameChatConversation(baseId: string, sessionId: string, name: string) {
    await this.prismaService.chatSession.update({
      where: { baseId, id: sessionId },
      data: { name },
    });
  }

  async completionsQuestions(sessionId: string, question: string) {
    const res = await this.prismaService.chatSession.findFirst({
      where: { id: sessionId },
    });
    if (!res) {
      throw new NotFoundException('chat session not found');
    }
    return this.prismaService.chatMessage.create({
      data: {
        id: generateChatMessageId(),
        sessionId,
        role: ChatMessageRole.Human,
        content: question,
        createdBy: this.cls.get('user.id'),
      },
    });
  }

  private combineAiMessageContent(data: IAiChatMessageContent[]) {
    const result: IAiChatMessageContent[] = [];
    data.forEach((item) => {
      const lastItem = result[result.length - 1];
      if (lastItem && item.node === lastItem.node) {
        if (item.type === ChatMessageDataType.SqlResult) {
          result[result.length - 1] = item;
        } else {
          lastItem.text += item.text;
        }
      } else {
        result.push(item);
      }
    });
    return result;
  }

  async completions(
    sessionId: string,
    messageId: string | undefined,
    data: IAiChatMessageContent[]
  ) {
    const res = await this.prismaService.chatSession.findFirst({
      where: { id: sessionId },
    });
    if (!res) {
      throw new NotFoundException('chat message not found');
    }
    if (messageId) {
      const message = await this.prismaService.chatMessage.findUnique({
        where: {
          id: messageId,
          sessionId,
          role: ChatMessageRole.Assistant,
        },
      });
      if (!message) {
        throw new NotFoundException(`Not found message: ${messageId}`);
      }
      const preContent = JSON.parse(message.content);
      return this.prismaService.chatMessage.update({
        where: {
          id: messageId,
          sessionId,
          role: ChatMessageRole.Assistant,
        },
        data: {
          content: JSON.stringify(this.combineAiMessageContent([...preContent, ...data])),
        },
        select: {
          id: true,
        },
      });
    }
    return this.prismaService.chatMessage.create({
      data: {
        id: generateChatMessageId(),
        sessionId,
        role: ChatMessageRole.Assistant,
        content: JSON.stringify(this.combineAiMessageContent(data)),
        createdBy: this.cls.get('user.id'),
      },
      select: {
        id: true,
      },
    });
  }
}
