-- Dormant negative backstop only. There is no runtime Ready journal writer,
-- physical cleanup, reader-closure or quota-release authority in this migration.
-- Reuse the existing reference extraction so a refusal rolls back the original
-- consumer write, its old-reference deletion and its revision change together.
CREATE TRIGGER IF NOT EXISTS managed_ready_ref_insert BEFORE INSERT ON managed_ref
WHEN EXISTS(SELECT 1 FROM managed_ready_retirement LIMIT 1)
BEGIN SELECT RAISE(ABORT,'pending ready retirement fences reference writes'); END;

CREATE TRIGGER IF NOT EXISTS managed_ready_ref_update BEFORE UPDATE ON managed_ref
WHEN EXISTS(SELECT 1 FROM managed_ready_retirement LIMIT 1)
BEGIN SELECT RAISE(ABORT,'pending ready retirement fences reference writes'); END;

-- Unknown payloads must retain a veto even when no literal path was extractable.
CREATE TRIGGER IF NOT EXISTS managed_ready_ref_blocked_insert BEFORE INSERT ON managed_ref_row
WHEN NEW.state='blocked' AND EXISTS(SELECT 1 FROM managed_ready_retirement LIMIT 1)
BEGIN SELECT RAISE(ABORT,'pending ready retirement fences ambiguous references'); END;

CREATE TRIGGER IF NOT EXISTS managed_ready_ref_blocked_update BEFORE UPDATE OF state ON managed_ref_row
WHEN NEW.state='blocked' AND EXISTS(SELECT 1 FROM managed_ready_retirement LIMIT 1)
BEGIN SELECT RAISE(ABORT,'pending ready retirement fences ambiguous references'); END;
