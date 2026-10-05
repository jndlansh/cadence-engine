import yaml from "yaml";
import { WorkflowSchema, WorkflowDefinition } from "./types.js";
import { DAGResolver } from "./dag.js";

export interface ParsedWorkflow {
  definition: WorkflowDefinition;
  resolver: DAGResolver;
}

export function parseWorkflowYAML(rawYaml: string): ParsedWorkflow {
  let parsedJson: unknown;
  try {
    parsedJson = yaml.parse(rawYaml);
  } catch (err) {
    throw new Error(`YAML Syntax Error: ${(err as Error).message}`);
  }

  // Runtime schema validation via Zod
  const validationResult = WorkflowSchema.safeParse(parsedJson);
  if (!validationResult.success) {
    const errorDetails = validationResult.error.issues
      .map((issue) => `${issue.path.join(".")}: ${issue.message}`)
      .join("; ");
    throw new Error(`Workflow Validation Failed: ${errorDetails}`);
  }

  const definition = validationResult.data;

  // Topological verification & cycle detection
  const resolver = new DAGResolver(definition.tasks);

  return {
    definition,
    resolver,
  };
}