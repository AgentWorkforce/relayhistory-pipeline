import { describe, expect, test, spyOn } from "bun:test";
import * as source from "../source.ts";
import sessions from "../../../packages/coordination-eval/fixtures/sessions-example.json";
import { runCoordinationReportJob, type ReportWorkerDeps } from "../worker.ts";
const request: source.ReportSourceRequest = {
  scope: { organizationId: "org", workspaceId: "ws" },
  window: {
    since: "2026-11-01T00:00:00.000Z",
    until: "2026-11-02T00:00:00.000Z",
  },
  maxSessions: 10,
  maxEvents: 20,
};
function setup() {
  const published: any[] = [];
  const failures: string[] = [];
  const deps: ReportWorkerDeps = {
    source: {
      trustedScope: request.scope,
      listSessions: async () => ({
        generation: "1",
        complete: true,
        sessions: [],
      }),
      now: () => "2026-11-03T00:00:00.000Z",
      canonical: {
        store: { backend: "s3" },
      } as source.ReportSourceDeps["canonical"],
    },
    database: {
      claim: async () => ({ jobId: "j1", leaseId: "lease", request }),
      complete: async (_claim, generation, artifact) => {
        published.push({ generation, artifact });
        return true;
      },
      fail: async (_claim, code) => {
        failures.push(code);
      },
    },
    transport: async () => {
      throw new Error("should not call model for empty window");
    },
  };
  return { deps, published, failures };
}
describe("leased coordination report worker", () => {
  test("publishes verified empty windows through durable source fence", async () => {
    const { deps, published } = setup();
    expect(await runCoordinationReportJob(deps, "j1")).toEqual({
      jobId: "j1",
      status: "completed",
      analysisStatus: "complete",
    });
    expect(published[0].generation).toBe("1");
    expect(published[0].artifact.markdown).toContain("unreviewed");
    expect(published[0].artifact.report.evidence).toBeUndefined();
    expect(published[0].artifact.report.evidenceArtifact).toBe(
      "assessment.evidence",
    );
  });
  test("does no work without claim and sanitizes claim failures", async () => {
    const { deps, published } = setup();
    deps.database.claim = async () => null;
    expect((await runCoordinationReportJob(deps, "j1")).status).toBe(
      "not_claimed",
    );
    deps.database.claim = async () => {
      throw new Error("private database data");
    };
    expect(
      JSON.stringify(await runCoordinationReportJob(deps, "j1")),
    ).not.toContain("private");
    expect(published).toHaveLength(0);
  });
  test("refuses source failures and stale publication without raw error leakage", async () => {
    const { deps, failures } = setup();
    deps.database.complete = async () => false;
    expect((await runCoordinationReportJob(deps, "j1")).errorCode).toBe(
      "publication_rejected",
    );
    deps.source.listSessions = async () => {
      throw new Error("private source data");
    };
    expect((await runCoordinationReportJob(deps, "j1")).errorCode).toBe(
      "source_failed",
    );
    expect(failures).toEqual(["publication_rejected", "source_failed"]);
  });
  test("renders unreviewed summaries and context requests with model markup escaped", async () => {
    const { deps, published } = setup();
    const mock = spyOn(source, "buildCanonicalReportExport").mockResolvedValue({
      export: sessions as any,
      sourceSnapshot: {
        directoryGeneration: "1",
        sessions: [],
        relationships: [],
      },
    });
    try {
      deps.transport = async ({ packet }) => ({
        packetId: packet.packetId,
        assessmentSha256: packet.assessmentSha256,
        workstreamId: packet.subject.workstreamId,
        workstreamSummary: {
          state: "unknown",
          text: "<script>Unreviewed summary</script>",
          evidenceIds: [packet.evidence[0]!.id],
        },
        observationReviews: [],
        candidateFindings: [],
        missingContextRequests: [
          {
            reason: "Outcome unknown.",
            sessionKeys: [],
            evidenceIds: [],
            requestedContext: "Request the outcome artifact.",
          },
        ],
      });
      expect((await runCoordinationReportJob(deps, "j1")).analysisStatus).toBe(
        "complete",
      );
      const markdown = published[0].artifact.markdown;
      expect(markdown).toContain(
        "&lt;script&gt;Unreviewed summary&lt;/script&gt;",
      );
      expect(markdown).not.toContain("<script>");
      expect(markdown).toContain("Request the outcome artifact.");
    } finally {
      mock.mockRestore();
    }
  });
  test("successful clipped packets remain explicitly partial semantic coverage", async () => {
    const { deps, published } = setup();
    const mock = spyOn(source, "buildCanonicalReportExport").mockResolvedValue({
      export: sessions as any,
      sourceSnapshot: {
        directoryGeneration: "1",
        sessions: [],
        relationships: [],
      },
    });
    try {
      deps.analysisOptions = {
        packetLimits: { maxEventChars: 1, maxEventsPerPacket: 1 },
      };
      deps.transport = async ({ packet }) => ({
        packetId: packet.packetId,
        assessmentSha256: packet.assessmentSha256,
        workstreamId: packet.subject.workstreamId,
        workstreamSummary: {
          state: "unknown",
          text: "Clipped context requires review.",
          evidenceIds: [packet.evidence[0]!.id],
        },
        observationReviews: [],
        candidateFindings: [],
        missingContextRequests: [],
      });
      const outcome = await runCoordinationReportJob(deps, "j1");
      expect(outcome.analysisStatus).toBe("partial");
      const artifact = published[0].artifact;
      expect(artifact.analysis.execution.completedPackets).toBe(
        artifact.analysis.execution.totalPackets,
      );
      expect(
        artifact.analysis.coverage.sampling.affectedPackets,
      ).toBeGreaterThan(0);
      expect(artifact.assessment.coverage.status).toBe("partial");
      expect(artifact.markdown).toContain("Bounded sampling:");
      expect(artifact.markdown).toContain("omitted event references");
    } finally {
      mock.mockRestore();
    }
  });
  test("upstream-truncated evidence keeps final report coverage partial", async () => {
    const { deps, published } = setup();
    const exported = structuredClone(sessions) as any;
    exported.events[0].truncated = true;
    const mock = spyOn(source, "buildCanonicalReportExport").mockResolvedValue({
      export: exported,
      sourceSnapshot: {
        directoryGeneration: "1",
        sessions: [],
        relationships: [],
      },
    });
    try {
      deps.transport = async ({ packet }) => ({
        packetId: packet.packetId,
        assessmentSha256: packet.assessmentSha256,
        workstreamId: packet.subject.workstreamId,
        workstreamSummary: {
          state: "unknown",
          text: "Upstream evidence is incomplete.",
          evidenceIds: [packet.evidence[0]!.id],
        },
        observationReviews: [],
        candidateFindings: [],
        missingContextRequests: [],
      });
      const outcome = await runCoordinationReportJob(deps, "j1");
      expect(outcome.analysisStatus).toBe("partial");
      expect(published[0].artifact.analysis.coverage.sampling.truncatedEvidence).toBeGreaterThan(0);
      expect(published[0].artifact.assessment.coverage.status).toBe("partial");
    } finally {
      mock.mockRestore();
    }
  });
  test("publishes failed semantic analysis only as explicitly partial draft coverage", async () => {
    const { deps, published } = setup();
    const mock = spyOn(source, "buildCanonicalReportExport").mockResolvedValue({
      export: sessions as any,
      sourceSnapshot: {
        directoryGeneration: "1",
        sessions: [],
        relationships: [],
      },
    });
    try {
      deps.transport = async () => {
        throw new Error("private provider data");
      };
      const result = await runCoordinationReportJob(deps, "j1");
      expect(result.analysisStatus).toBe("partial");
      expect(published[0].artifact.analysis.coverage.status).toBe("partial");
      expect(published[0].artifact.assessment.coverage.status).toBe("partial");
      expect(published[0].artifact.markdown).toContain(
        "Analysis coverage: partial",
      );
      expect(JSON.stringify(published)).not.toContain("private provider data");
    } finally {
      mock.mockRestore();
    }
  });
});
