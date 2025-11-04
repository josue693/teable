-- RedefineTables
PRAGMA defer_foreign_keys=ON;
PRAGMA foreign_keys=OFF;
CREATE TABLE "new_users" (
    "id" TEXT NOT NULL PRIMARY KEY,
    "account_name" TEXT,
    "name" TEXT NOT NULL,
    "password" TEXT,
    "salt" TEXT,
    "phone" TEXT,
    "email" TEXT,
    "avatar" TEXT,
    "is_system" BOOLEAN,
    "is_admin" BOOLEAN,
    "is_trial_used" BOOLEAN,
    "notify_meta" TEXT,
    "last_sign_time" DATETIME,
    "deactivated_time" DATETIME,
    "created_time" DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "deleted_time" DATETIME,
    "last_modified_time" DATETIME,
    "permanent_deleted_time" DATETIME,
    "ref_meta" TEXT
);
INSERT INTO "new_users" ("account_name", "avatar", "created_time", "deactivated_time", "deleted_time", "email", "id", "is_admin", "is_system", "is_trial_used", "last_modified_time", "last_sign_time", "name", "notify_meta", "password", "permanent_deleted_time", "phone", "ref_meta", "salt") 
SELECT 
    LOWER(CASE 
        WHEN substr("id", 1, 3) = 'usr' THEN 'acn' || substr("id", 4)
        ELSE "id"
    END),
    "avatar", "created_time", "deactivated_time", "deleted_time", "email", "id", "is_admin", "is_system", "is_trial_used", "last_modified_time", "last_sign_time", "name", "notify_meta", "password", "permanent_deleted_time", "phone", "ref_meta", "salt" 
FROM "users";
DROP TABLE "users";
ALTER TABLE "new_users" RENAME TO "users";
CREATE UNIQUE INDEX "users_account_name_key" ON "users"("account_name");
CREATE UNIQUE INDEX "users_phone_key" ON "users"("phone");
CREATE UNIQUE INDEX "users_email_key" ON "users"("email");
PRAGMA foreign_keys=ON;
PRAGMA defer_foreign_keys=OFF;
