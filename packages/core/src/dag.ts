import { TaskDefinition } from "./types.js";

export class DAGResolver {
  private tasks: Map<string, TaskDefinition>;
  private inDegree: Map<string, number>;
  private adjacencyList: Map<string, string[]>; // parentId -> childIds[]

  constructor(tasks: TaskDefinition[]) {
    this.tasks = new Map(tasks.map((t) => [t.id, t]));
    this.inDegree = new Map();
    this.adjacencyList = new Map();

    this.validateTaskReferences();
    this.buildGraph();
    this.detectCycles();
  }

  /**
   * Ensure no task depends on an ID that does not exist in the workflow.
   */
  private validateTaskReferences(): void {
    for (const task of this.tasks.values()) {
      for (const parentId of task.depends_on) {
        if (!this.tasks.has(parentId)) {
          throw new Error(
            `Task "${task.id}" depends on undefined task "${parentId}".`
          );
        }
      }
    }
  }

  /**
   * Build in-degree counts and parent-to-child adjacency graph.
   */
  private buildGraph(): void {
    for (const id of this.tasks.keys()) {
      this.inDegree.set(id, 0);
      this.adjacencyList.set(id, []);
    }

    for (const task of this.tasks.values()) {
      this.inDegree.set(task.id, task.depends_on.length);
      for (const parentId of task.depends_on) {
        this.adjacencyList.get(parentId)!.push(task.id);
      }
    }
  }

  /**
   * Kahn's Algorithm for cycle detection.
   * If processed nodes count != total nodes, a cycle exists.
   */
  private detectCycles(): void {
    const tempInDegree = new Map(this.inDegree);
    const queue: string[] = [];

    for (const [id, degree] of tempInDegree.entries()) {
      if (degree === 0) {
        queue.push(id);
      }
    }

    let processedCount = 0;

    while (queue.length > 0) {
      const current = queue.shift()!;
      processedCount++;

      for (const neighbor of this.adjacencyList.get(current)!) {
        const updatedDegree = tempInDegree.get(neighbor)! - 1;
        tempInDegree.set(neighbor, updatedDegree);
        if (updatedDegree === 0) {
          queue.push(neighbor);
        }
      }
    }

    if (processedCount !== this.tasks.size) {
      throw new Error("Invalid Workflow: Cycle detected in task dependency graph.");
    }
  }

  /**
   * Returns all root tasks that have zero dependencies and can run immediately.
   */
  public getInitialTasks(): TaskDefinition[] {
    const readyTasks: TaskDefinition[] = [];
    for (const [id, degree] of this.inDegree.entries()) {
      if (degree === 0) {
        readyTasks.push(this.tasks.get(id)!);
      }
    }
    return readyTasks;
  }

  /**
   * Given a set of completed task IDs, returns tasks whose dependencies are now fully met.
   */
  public getNextExecutableTasks(completedTaskIds: Set<string>): TaskDefinition[] {
    const executable: TaskDefinition[] = [];

    for (const task of this.tasks.values()) {
      if (completedTaskIds.has(task.id)) {
        continue; // Already finished
      }

      // Check if all parent tasks are completed
      const allParentsCompleted = task.depends_on.every((parentId) =>
        completedTaskIds.has(parentId)
      );

      if (allParentsCompleted) {
        executable.push(task);
      }
    }

    return executable;
  }
}