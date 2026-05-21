import { ImageResponse } from "@takumi-rs/image-response";
import { notFound } from "next/navigation";
import { getPageImage, source } from "@/lib/source";

const brandColor = "rgb(241, 120, 41)";
const brandBorderColor = "rgba(241, 120, 41, 0.3)";

function KitaruOgImage({
  title,
  description,
}: {
  title: string;
  description?: string;
}) {
  return (
    <div
      style={{
        display: "flex",
        flexDirection: "column",
        width: "100%",
        height: "100%",
        color: "white",
        padding: "4rem",
        backgroundColor: "#0c0c0c",
        border: `18px solid ${brandBorderColor}`,
      }}
    >
      <p
        style={{
          fontWeight: 800,
          fontSize: "82px",
          margin: 0,
        }}
      >
        {title}
      </p>
      <p
        style={{
          fontSize: "52px",
          color: "rgba(240, 240, 240, 0.8)",
          margin: 0,
          marginTop: "16px",
          paddingBottom: "28px",
          borderBottomWidth: "8px",
          borderBottomStyle: "solid",
          borderBottomColor: brandBorderColor,
        }}
      >
        {description ?? "Durable execution for agent workflows."}
      </p>
      <div
        style={{
          display: "flex",
          flexDirection: "row",
          alignItems: "center",
          gap: "20px",
          marginTop: "auto",
          color: brandColor,
        }}
      >
        <svg
          xmlns="http://www.w3.org/2000/svg"
          width="56"
          height="56"
          viewBox="0 0 24 24"
          fill="none"
          stroke="currentColor"
          strokeWidth="2"
          strokeLinecap="round"
          strokeLinejoin="round"
        >
          <title>Kitaru</title>
          <circle cx="12" cy="12" r="11" stroke={brandColor} strokeWidth="2" />
        </svg>
        <p
          style={{
            fontSize: "56px",
            fontWeight: 600,
            margin: 0,
          }}
        >
          Kitaru
        </p>
      </div>
    </div>
  );
}

export const revalidate = false;

export async function GET(
  _req: Request,
  { params }: RouteContext<"/og/docs/[...slug]">,
) {
  const { slug } = await params;
  const page = source.getPage(slug.slice(0, -1));
  if (!page) notFound();

  return new ImageResponse(
    <KitaruOgImage
      title={page.data.title}
      description={page.data.description}
    />,
    {
      width: 1200,
      height: 630,
      format: "webp",
    },
  );
}

export function generateStaticParams() {
  return source.getPages().map((page) => ({
    lang: page.locale,
    slug: getPageImage(page).segments,
  }));
}
