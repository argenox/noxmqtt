import Head from '@docusaurus/Head';
import {Redirect} from '@docusaurus/router';
import useDocusaurusContext from '@docusaurus/useDocusaurusContext';

const HOME_DESCRIPTION =
  'Official NoxMQTT documentation: MQTT 3.1.1 and MQTT 5 in C for embedded systems, including secure transport guidance, ESP-IDF integration, and public API docs.';

export default function Home() {
  const {siteConfig} = useDocusaurusContext();
  const canonical = `${siteConfig.url}/docs/intro`;

  return (
    <>
      <Head>
        <title>{siteConfig.title} | Embedded MQTT Client in C</title>
        <meta name="description" content={HOME_DESCRIPTION} />
        <meta name="robots" content="index, follow" />
        <link rel="canonical" href={canonical} />
        <meta property="og:title" content={`${siteConfig.title} | Embedded MQTT Client in C`} />
        <meta property="og:description" content={HOME_DESCRIPTION} />
        <meta property="og:url" content={canonical} />
        <meta property="og:type" content="website" />
        <meta name="twitter:card" content="summary" />
        <meta name="twitter:title" content={`${siteConfig.title} | Embedded MQTT Client in C`} />
        <meta name="twitter:description" content={HOME_DESCRIPTION} />
      </Head>
      <Redirect to="/docs/intro" />
    </>
  );
}
