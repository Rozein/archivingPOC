SELECT * FROM "AssignmentAttachment" WHERE "AssignmentId" = ANY($1::uuid[]) order by "CreationTime" desc;
