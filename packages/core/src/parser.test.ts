import { describe, it, expect } from "vitest";
import { parseWorkflowYAML } from "./parser.js";

describe("parseWorkflowYAML", () => {
  it("should successfully parse and validate a valid workflow YAML", () => {
    const validYaml = `
version: "1.0"
workflow: nightly_backup
tasks:
  - id: dump_db
    command: "echo dumping db"
  - id: compress
    command: "echo compressing"
    depends_on:
      - dump_db
`;

    const { definition, resolver } = parseWorkflowYAML(validYaml);

    expect(definition.workflow).toBe("nightly_backup");
    expect(definition.tasks.length).toBe(2);
    expect(resolver.getInitialTasks().map((t) => t.id)).toEqual(["dump_db"]);
  });

  it("should fail on invalid YAML structure (missing tasks)", () => {
    const invalidYaml = `
version: "1.0"
workflow: empty_workflow
tasks: []
`;

    expect(() => parseWorkflowYAML(invalidYaml)).toThrow(
      "Workflow must define at least one task"
    );
  });

  it("should catch cycles defined inside YAML", () => {
    const cyclicYaml = `
version: "1.0"
workflow: cyclic_flow
tasks:
  - id: task_1
    command: "echo 1"
    depends_on: ["task_2"]
  - id: task_2
    command: "echo 2"
    depends_on: ["task_1"]
`;

    expect(() => parseWorkflowYAML(cyclicYaml)).toThrow(
      "Invalid Workflow: Cycle detected in task dependency graph."
    );
  });
});