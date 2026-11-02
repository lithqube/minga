// Technical writing by the maintainer, linked (not copied): canonical copies stay on Substack.
export interface Article {
  title: string;
  url: string;
  date: string;
  summary: string;
}

export const articles: Article[] = [
  {
    title: '“It Works” Is Not an Architecture',
    url: 'https://devferru.substack.com/p/it-works-is-not-an-architecture',
    date: '2026-10-23',
    summary:
      'Managed defaults become your architecture whether anyone chose them or not. Ask for evidence, not configuration.',
  },
  {
    title: 'Your Agent Will Confidently Hand-Roll a Dead-Letter Queue That Doesn’t Exist',
    url: 'https://devferru.substack.com/p/your-agent-will-confidently-hand',
    date: '2026-09-18',
    summary: 'JetStream has no built-in DLQ. max_deliver, advisory capture and payload retention, tested with evals.',
  },
  {
    title: 'Production Has No Keys to Give: AI Agents in Your Deploy Pipeline',
    url: 'https://devferru.substack.com/p/production-has-no-keys-to-give-ai',
    date: '2026-07-20',
    summary: 'GitOps, signed artifacts and policy-gated rollouts so the agent holds nothing it can misuse.',
  },
  {
    title: 'The Insider Threat You Just Hired: AI Agents in Production',
    url: 'https://devferru.substack.com/p/the-insider-threat-you-just-hired',
    date: '2026-05-22',
    summary: 'Model AI agents as identities: scoped subjects on the fabric, provenance-aware policy, audit.',
  },
  {
    title: 'Zero Trust Architecture Your Boss Thinks You Already Have',
    url: 'https://devferru.substack.com/p/zero-trust-architecture-your-boss',
    date: '2026-04-10',
    summary:
      'Network location is not an identity. NATS mTLS and JWT versus a sidecar mesh, and why rotation is where it dies.',
  },
  {
    title: 'Securing Your Microservices: Why Static Credentials Still Cause Breaches',
    url: 'https://devferru.substack.com/p/securing-your-microservices-why-static',
    date: '2026-03-17',
    summary: 'Ory, Vault, OPA and NATS as layers: dynamic credentials, policy as code, encrypted transport.',
  },
  {
    title: 'From Service Chaos to Detective Genius: Your Cloud-Native Investigation Toolkit',
    url: 'https://devferru.substack.com/p/from-service-chaos-to-detective-genius',
    date: '2026-02-20',
    summary: 'Metrics, logs and traces for a NATS-based system, and alerting on symptoms instead of causes.',
  },
  {
    title: 'The Cloud-Native Chronicles: Catching Up Before We Level Up',
    url: 'https://devferru.substack.com/p/the-cloud-native-chronicles-catching',
    date: '2026-01-28',
    summary: 'The series so far: brokers, containers and NATS as a lightweight service mesh.',
  },
];

/** The three linked from the landing architecture section. */
export const featuredArticles = articles.slice(0, 3);
