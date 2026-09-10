import { defineConfig } from "astro/config";
import starlight from "@astrojs/starlight";
import starlightVersions from "starlight-versions";
import starlightCelestiaTheme from "starlight-theme-celestia";

export default defineConfig({
  site: "https://natlabrockies.github.io/R2X/",
  base: "/R2X/",
  integrations: [
    starlight({
      title: "R2X",
      description: "Translation plugins for ReEDS, PLEXOS, and Sienna model interoperability.",
      logo: {
        light: "./src/assets/r2x-logo-full-color.svg",
        dark: "./src/assets/r2x-logo-white.svg",
        alt: "R2X",
      },
      social: [{ icon: "github", label: "GitHub", href: "https://github.com/NatLabRockies/R2X" }],
      editLink: {
        baseUrl: "https://github.com/NatLabRockies/R2X/edit/main/",
      },
      customCss: ["./src/styles/brand.css"],
      components: {
        SiteTitle: "./src/components/SiteTitleVersioned.astro",
      },
      plugins: [
        starlightCelestiaTheme(),
        starlightVersions({
          current: { label: "v2.2.0" },
          versions: [{ slug: "2-0-0", label: "v2.0.0" }],
        }),
      ],
      sidebar: [
        {
          label: "Start here",
          items: [
            { label: "Introduction", link: "/" },
            { label: "Getting started", link: "/tutorials/getting-started/" },
          ],
        },
        {
          label: "Translation workflows",
          items: [
            { label: "Workflow overview", link: "/how-to/" },
            { label: "ReEDS to PLEXOS", link: "/how-to/reeds-to-plexos/" },
            { label: "ReEDS to Sienna", link: "/how-to/reeds-to-sienna/" },
            { label: "PLEXOS to Sienna", link: "/how-to/plexos-to-sienna/" },
            { label: "Sienna to PLEXOS", link: "/how-to/sienna-to-plexos/" },
          ],
        },
        {
          label: "Reference",
          items: [{ label: "Changelog", link: "/reference/changelog/" }],
        },
        {
          label: "Explanation",
          items: [{ label: "Architecture", link: "/explanation/architecture/" }],
        },
        {
          label: "Contributors",
          items: [{ label: "Development", link: "/how-to/development/" }],
        },
      ],
    }),
  ],
});
