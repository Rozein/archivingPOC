SELECT * FROM "AssignmentSubmission" WHERE "AssignmentId" = ANY($1::uuid[]);
