-- AddForeignKey
ALTER TABLE "reference" ADD CONSTRAINT "reference_from_field_id_fkey" FOREIGN KEY ("from_field_id") REFERENCES "field"("id") ON DELETE RESTRICT ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "reference" ADD CONSTRAINT "reference_to_field_id_fkey" FOREIGN KEY ("to_field_id") REFERENCES "field"("id") ON DELETE RESTRICT ON UPDATE CASCADE;
