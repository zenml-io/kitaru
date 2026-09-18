import { ImageResponse } from "@takumi-rs/image-response";
import { notFound } from "next/navigation";
import { getPageImage, source } from "@/lib/source";

const brandBorderColor = "rgba(244, 147, 79, 0.3)";

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
          color: "white",
        }}
      >
        <svg
          xmlns="http://www.w3.org/2000/svg"
          width="56"
          height="54"
          viewBox="0 0 259.41 249.425"
          fill="none"
        >
          <title>Kitaru</title>
          <path
            d="M127.9 160.625C129 160.625 130.2 160.325 131.2 159.925L249 103.525C255.2 100.525 259.3 94.125 259.3 87.225V71.025C259.3 64.025 255.4 57.825 249.1 54.825L138.8 2.025C133 -0.675 126.4 -0.675 120.7 2.025L10.3 54.825C4.1 57.725 0 64.125 0 71.125V87.325C0 94.325 3.9 100.525 10.2 103.525L55.71 125.245C55.71 125.245 22.16 141.255 10.98 146.595C4.11 149.875 0.0100021 155.725 0.0100021 163.225V178.425C0.0100021 185.425 3.91 191.625 10.21 194.625L120.51 247.325C123.41 248.725 126.51 249.425 129.61 249.425C132.71 249.425 135.81 248.725 138.71 247.325L249.11 194.625C255.31 191.625 259.41 185.225 259.41 178.325V162.125C259.41 155.125 255.51 148.925 249.21 145.925L224.81 134.325L207.11 142.925L242.51 159.825C243.41 160.325 244.01 161.225 244.01 162.225V178.425C244.01 179.425 243.41 180.425 242.51 180.825L132.21 233.525C130.61 234.225 128.91 234.225 127.31 233.525L17.01 180.825C16.11 180.325 15.51 179.425 15.51 178.425V163.225C15.51 161.725 16.71 160.625 18.11 160.625C18.11 160.625 100.46 160.625 127.91 160.625H127.9ZM15.4 87.225V71.025C15.4 70.025 16 69.125 16.9 68.625L127.3 15.925C128.9 15.225 130.6 15.225 132.2 15.925L242.5 68.625C243.4 69.125 244 70.025 244 71.025V87.225C244 88.225 243.4 89.225 242.5 89.625L132.2 142.325C130.6 143.025 128.9 143.025 127.3 142.325L16.9 89.625C16 89.125 15.4 88.225 15.4 87.225Z"
            fill="currentColor"
          />
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
