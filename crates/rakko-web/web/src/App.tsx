import { useCallback, useEffect, useMemo, useState } from "react";
import { api, type Profile, type Summary, type Topic } from "./api";
import { Brokers } from "./Brokers";
import { Detail } from "./Detail";
import { formatCount } from "./format";
import { GroupList, GroupView } from "./Groups";
import { RegisterDialog, SchemasHome, SubjectList, SubjectView } from "./Schemas";
import { CreateTopicDialog } from "./TopicAdmin";
import { TopicView, type ProduceInitial } from "./TopicView";

type View = "topics" | "groups" | "schemas" | "brokers";
type Route = { profile: string; view: View; item: string };

// #/<profile>/<topics|groups|brokers>/<item>
function readRoute(): Route {
  const [profile = "", view = "topics", item = ""] = window.location.hash.replace(/^#\/?/, "").split("/").map(decodeURIComponent);
  return { profile, view: view === "groups" || view === "schemas" || view === "brokers" ? view : "topics", item };
}

function writeRoute(r: Route) {
  const parts = [r.profile, r.view, ...(r.item ? [r.item] : [])].map(encodeURIComponent);
  const hash = r.profile ? `#/${parts.join("/")}` : "";
  if (window.location.hash !== hash) window.location.hash = hash;
}

const message = (e: unknown) => (e instanceof Error ? e.message : String(e));

export function App() {
  const [route, setRoute] = useState<Route>(readRoute);
  const [profiles, setProfiles] = useState<Profile[] | null>(null);
  const [topics, setTopics] = useState<Topic[] | null>(null);
  const [topicFilter, setTopicFilter] = useState("");
  const [error, setError] = useState<string | null>(null);
  const [selected, setSelected] = useState<Summary | null>(null);
  const [produceFrom, setProduceFrom] = useState<ProduceInitial | null>(null);
  const [subjectsKey, setSubjectsKey] = useState(0);
  const [newSubject, setNewSubject] = useState(false);
  const [newTopic, setNewTopic] = useState(false);

  // <details> menus close when you click elsewhere (or pick an item).
  useEffect(() => {
    const onClick = (e: MouseEvent) => {
      document.querySelectorAll<HTMLDetailsElement>("details.menu[open]").forEach((d) => {
        const inside = d.contains(e.target as Node);
        const item = inside && (e.target as HTMLElement).closest("summary") === null;
        if (!inside || item) d.open = false;
      });
    };
    document.addEventListener("click", onClick);
    return () => document.removeEventListener("click", onClick);
  }, []);

  useEffect(() => {
    const onHash = () => setRoute(readRoute());
    window.addEventListener("hashchange", onHash);
    return () => window.removeEventListener("hashchange", onHash);
  }, []);

  useEffect(() => {
    api
      .profiles()
      .then((p) => {
        setProfiles(p);
        if (!route.profile && p.length > 0) writeRoute({ profile: p[0].name, view: "topics", item: "" });
      })
      .catch((e: unknown) => setError(message(e)));
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  const loadTopics = useCallback(() => {
    if (!route.profile) return;
    api.topics(route.profile).then(setTopics).catch((e: unknown) => setError(message(e)));
  }, [route.profile]);

  useEffect(() => {
    setTopics(null);
    setError(null);
    loadTopics();
  }, [loadTopics]);

  useEffect(() => setSelected(null), [route.profile, route.view, route.item]);

  const visibleTopics = useMemo(() => {
    const needle = topicFilter.trim().toLowerCase();
    return (topics ?? []).filter((t) => t.name.toLowerCase().includes(needle));
  }, [topics, topicFilter]);

  const go = (view: View, item = "") => writeRoute({ profile: route.profile, view, item });
  const showTopic = route.view === "topics" && route.item;
  const hasSidebar = route.view !== "brokers";

  return (
    <div className={`app${selected && showTopic ? " with-detail" : ""}${hasSidebar ? "" : " no-side"}`}>
      <header className="topbar">
        <img className="logo" src="/otter.jpg" alt="" />
        <span className="brand">rakko</span>
        <select value={route.profile} onChange={(e) => writeRoute({ profile: e.target.value, view: route.view, item: "" })} aria-label="cluster profile">
          {profiles?.map((p) => (
            <option key={p.name} value={p.name}>
              {p.name}
            </option>
          ))}
        </select>
        <nav className="tabs" aria-label="sections">
          {(["topics", "groups", "schemas", "brokers"] as View[]).map((v) => (
            <button key={v} className={route.view === v ? "on" : undefined} aria-current={route.view === v} onClick={() => go(v)}>
              {v}
            </button>
          ))}
        </nav>
        {profiles?.find((p) => p.name === route.profile)?.has_schema_registry === false && (
          <span className="warn small">no schema registry — Avro is shown undecoded</span>
        )}
      </header>

      {hasSidebar && (
        <nav className="sidebar">
          {route.view === "topics" ? (
            <>
              <input placeholder="filter topics…" value={topicFilter} onChange={(e) => setTopicFilter(e.target.value)} spellCheck={false} />
              <button className="link new" onClick={() => setNewTopic(true)}>
                + new topic
              </button>
              {error && <div className="err pad">{error}</div>}
              {!topics && !error && <div className="dim pad">loading topics…</div>}
              <ul>
                {visibleTopics.map((t) => (
                  <li key={t.name}>
                    <button className={t.name === route.item ? "topic active" : "topic"} onClick={() => go("topics", t.name)}>
                      <span className="name">{t.name}</span>
                      <span className="count">{formatCount(t.messages)}</span>
                    </button>
                  </li>
                ))}
              </ul>
            </>
          ) : route.view === "schemas" ? (
            route.profile && (
              <SubjectList
                profile={route.profile}
                selected={route.item}
                onSelect={(x) => go("schemas", x)}
                refreshKey={subjectsKey}
                onCreate={() => setNewSubject(true)}
              />
            )
          ) : (
            route.profile && <GroupList profile={route.profile} selected={route.item} onSelect={(g) => go("groups", g)} />
          )}
        </nav>
      )}

      <main>
        {!route.profile ? (
          <Placeholder text="No cluster profiles are configured." />
        ) : route.view === "brokers" ? (
          <Brokers profile={route.profile} />
        ) : route.view === "schemas" ? (
          route.item ? (
            <SubjectView
              profile={route.profile}
              subject={route.item}
              onChanged={() => setSubjectsKey((k) => k + 1)}
              onGone={() => go("schemas")}
            />
          ) : (
            <SchemasHome profile={route.profile} onChanged={() => setSubjectsKey((k) => k + 1)} onCreated={(x) => go("schemas", x)} />
          )
        ) : route.view === "groups" ? (
          route.item ? (
            <GroupView profile={route.profile} group={route.item} onOpenTopic={(t) => go("topics", t)} />
          ) : (
            <Placeholder text="Pick a consumer group." />
          )
        ) : route.item ? (
          <TopicView
            profile={route.profile}
            topic={route.item}
            info={topics?.find((t) => t.name === route.item)}
            selected={selected}
            onSelect={setSelected}
            onChanged={loadTopics}
            onDeleted={() => {
              loadTopics();
              go("topics");
            }}
            produceFrom={produceFrom}
            onProduceFrom={setProduceFrom}
          />
        ) : (
          <Placeholder text="Pick a topic." />
        )}
      </main>

      {newTopic && route.profile && (
        <CreateTopicDialog
          profile={route.profile}
          onClose={() => setNewTopic(false)}
          onDone={(name) => {
            loadTopics();
            go("topics", name);
          }}
        />
      )}

      {newSubject && route.profile && (
        <RegisterDialog
          profile={route.profile}
          onClose={() => setNewSubject(false)}
          onDone={(x) => {
            setSubjectsKey((k) => k + 1);
            go("schemas", x);
          }}
        />
      )}

      {selected && showTopic && (
        <Detail
          profile={route.profile}
          topic={route.item}
          message={selected}
          onClose={() => setSelected(null)}
          onProduceFrom={setProduceFrom}
          onReplayed={loadTopics}
        />
      )}
    </div>
  );
}

function Placeholder({ text }: { text: string }) {
  return (
    <div className="placeholder">
      <img src="/otter.jpg" alt="" />
      <div>{text}</div>
    </div>
  );
}
