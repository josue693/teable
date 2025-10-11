import { Logger } from '@nestjs/common';

export enum QueueConsumerType {
  Automation = 'automation',
  ImportExport = 'import-export',
  ImageCrop = 'image-crop',
  Default = 'default',
}

export function conditionalQueueProcessorProviders<T>(
  ...opts: {
    consumer?: QueueConsumerType;
    providers: T[];
  }[]
): T[] {
  if (process.env.BACKEND_DISABLE_QUEUE_CONSUMER === 'true') {
    return [];
  }
  const selectedConsumer = (process.env.BACKEND_QUEUE_CONSUMER?.split(',')?.filter((v) =>
    Object.values(QueueConsumerType).includes(v as QueueConsumerType)
  ) || []) as QueueConsumerType[];

  // If selected consumer is provided, return providers for the selected consumer
  if (selectedConsumer.length > 0) {
    const providers: T[] = [];
    for (const opt of opts) {
      const consumer = opt.consumer || QueueConsumerType.Default;
      if (selectedConsumer.includes(consumer)) {
        providers.push(...opt.providers);
      }
    }
    providers.length > 0 &&
      Logger.log(
        `Queue Consumer Providers (${selectedConsumer.join(', ')}): ${providers.map((p) => (typeof p === 'function' ? p.name : 'unknown')).join(', ')}`
      );
    return providers;
  }

  // If no selected consumer is provided, return providers for all consumers
  const providers: T[] = [];
  for (const opt of opts) {
    providers.push(...opt.providers);
  }
  providers.length > 0 &&
    Logger.log(
      `Queue Consumer Providers (ALL): ${providers.map((p) => (typeof p === 'function' ? p.name : 'unknown')).join(', ')}`
    );
  return providers;
}
