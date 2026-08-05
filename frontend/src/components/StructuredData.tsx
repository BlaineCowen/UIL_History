import { SITE_DESCRIPTION, SITE_NAME, SITE_URL, canonical } from "@/lib/site";

/**
 * JSON-LD. Search engines infer what a page is from its text; this states it.
 *
 * Rendered as a plain <script type="application/ld+json"> — that content type
 * is data, not executable, so it is not an injection vector the way a real
 * script would be. Values are JSON-encoded rather than interpolated.
 */
function Ld({ data }: { data: Record<string, unknown> }) {
  return (
    <script
      type="application/ld+json"
      dangerouslySetInnerHTML={{ __html: JSON.stringify(data).replace(/</g, "\\u003c") }}
    />
  );
}

/** Site-wide identity plus a search action, on every page via the layout. */
export function SiteStructuredData() {
  return (
    <>
      <Ld
        data={{
          "@context": "https://schema.org",
          "@type": "WebSite",
          name: SITE_NAME,
          url: SITE_URL,
          description: SITE_DESCRIPTION,
          potentialAction: {
            "@type": "SearchAction",
            target: {
              "@type": "EntryPoint",
              urlTemplate: `${SITE_URL}/pml?q={search_term_string}`,
            },
            "query-input": "required name=search_term_string",
          },
        }}
      />
      <Ld
        data={{
          "@context": "https://schema.org",
          "@type": "Dataset",
          name: "Texas UIL Concert & Sight-Reading results and Prescribed Music List",
          description:
            "Contest results for Texas UIL Concert & Sight-Reading from 2005 " +
            "onward, matched against the official Prescribed Music List.",
          url: canonical("/pml"),
          keywords: [
            "UIL PML",
            "Texas UIL prescribed music list",
            "UIL concert and sight-reading",
          ],
          temporalCoverage: "2005/..",
          spatialCoverage: { "@type": "Place", name: "Texas, United States" },
          isAccessibleForFree: true,
          creator: { "@type": "Person", name: "Blaine Cowen" },
        }}
      />
    </>
  );
}

/** One piece: what it is, its grade, and how it has actually performed. */
export function SongStructuredData({
  title,
  composer,
  eventName,
  grade,
  code,
  performances,
  averageConcert,
}: {
  title: string;
  composer?: string | null;
  eventName: string;
  grade: number;
  code: string;
  performances: number;
  averageConcert?: number | null;
}) {
  const data: Record<string, unknown> = {
    "@context": "https://schema.org",
    "@type": "MusicComposition",
    name: title,
    url: canonical(`/pml/${encodeURIComponent(code)}`),
    genre: `UIL ${eventName}`,
    identifier: code,
    additionalProperty: [
      { "@type": "PropertyValue", name: "UIL grade", value: String(grade) },
      { "@type": "PropertyValue", name: "UIL event", value: eventName },
      {
        "@type": "PropertyValue",
        name: "UIL contest performances",
        value: String(performances),
      },
    ],
  };
  if (composer) data.composer = { "@type": "Person", name: composer };
  if (averageConcert) {
    (data.additionalProperty as unknown[]).push({
      "@type": "PropertyValue",
      name: "Average concert rating (1 best, 5 worst)",
      value: averageConcert.toFixed(2),
    });
  }
  return <Ld data={data} />;
}
