/*
  Warnings:

  - A unique constraint covering the columns `[account_name]` on the table `users` will be added. If there are existing duplicate values, this will fail.
  - Made the column `account_name` on table `users`.

*/
-- AlterTable
ALTER TABLE "users" ADD COLUMN "account_name" TEXT;

-- Update existing users with account_name
UPDATE "users" 
SET "account_name" = LOWER(CASE 
    WHEN substr("id", 1, 3) = 'usr' THEN 'acn' || substr("id", 4)
    ELSE "id"
END)
WHERE "account_name" IS NULL;

-- Make email nullable
ALTER TABLE "users" ALTER COLUMN "email" DROP NOT NULL;

-- CreateIndex
CREATE UNIQUE INDEX "users_account_name_key" ON "users"("account_name");
