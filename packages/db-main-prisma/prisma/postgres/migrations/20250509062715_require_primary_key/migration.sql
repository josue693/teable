/*
  Warnings:

  - You are about to drop the `snapshots` table. If the table is not empty, all the data it contains will be lost.
  - The required column `id` was added to the `comment_subscription` table with a prisma-level default value. This is not possible if the table is not empty. Please add this column as optional, then populate it before making it required.
  - The required column `id` was added to the `ops` table with a prisma-level default value. This is not possible if the table is not empty. Please add this column as optional, then populate it before making it required.
  - The required column `id` was added to the `plugin_context_menu` table with a prisma-level default value. This is not possible if the table is not empty. Please add this column as optional, then populate it before making it required.

*/
-- AlterTable
BEGIN;

ALTER TABLE "comment_subscription" ADD COLUMN     "id" TEXT NOT NULL,
ADD CONSTRAINT "comment_subscription_pkey" PRIMARY KEY ("id");

-- AlterTable
ALTER TABLE "ops" ADD COLUMN     "id" TEXT NOT NULL,
ADD CONSTRAINT "ops_pkey" PRIMARY KEY ("id");

-- AlterTable
ALTER TABLE "plugin_context_menu" ADD COLUMN     "id" TEXT NOT NULL,
ADD CONSTRAINT "plugin_context_menu_pkey" PRIMARY KEY ("id");

-- DropTable
DROP TABLE "snapshots";

COMMIT;
