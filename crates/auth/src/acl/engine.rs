//! Rules compiled for deciding, once, and bound to each client, once per connection.
//!
//! A decision for a bound client walks the rules in order and, for each, tests two bits: is
//! the rule one of this client's, decided when it connected by its user name, identifier,
//! address and attributes, and does the rule cover this action at this QoS and RETAIN. Only a
//! rule passing both looks at topics: first a key packed from each topic's first level, which
//! rules out most topics with one comparison, then the topic itself, and the first rule whose
//! topic matches decides. Regular expressions, networks and placeholders are dealt with when
//! the client binds, never per message.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, PoisonError, RwLock};

use openqtt_core::{QoS, TopicFilter, TopicName};
use openqtt_ext::{Action, Authorizer, ClientAuthorizer, ClientInfo, Permission};
use openqtt_topic::MAX_TOPIC_LEN;
use regex::{Regex, RegexBuilder};

use super::spec::{Cidr, Decision, NameMatch, RuleSpec, TopicSpec, Who, canonical};
use super::topic::{Template, TemplateError, covers, denies};
use crate::Error;

/// The most a compiled regular expression may take, so a rule cannot hold megabytes per name.
const REGEX_SIZE_LIMIT: usize = 1 << 20;

/// The rules of an ACL file, compiled: the first rule that matches decides, and no match is a
/// deny (R2 rule 9).
#[derive(Debug)]
pub struct Acl {
    rules: Box<[Rule]>,
}

#[derive(Debug)]
struct Rule {
    decision: Decision,
    who: Box<[Condition]>,
    /// The actions it covers, one bit for each kind, QoS and RETAIN ([`request`]).
    actions: u16,
    topics: Box<[Topic]>,
}

#[derive(Debug)]
enum Condition {
    Username(Box<[Matcher]>),
    ClientId(Box<[Matcher]>),
    Address(Box<[Cidr]>),
    Attribute(Box<str>, Box<[Matcher]>),
}

#[derive(Debug)]
enum Matcher {
    Exact(Box<str>),
    Prefix(Box<str>),
    Regex(Regex),
}

#[derive(Debug)]
enum Topic {
    Filter {
        filter: TopicFilter,
        first: Option<LevelKey>,
    },
    Template {
        template: Template,
        slot: usize,
    },
    Exact {
        text: Box<str>,
        first: LevelKey,
    },
    All,
}

/// A topic's filter for one client, where the rule wrote placeholders.
#[derive(Clone, Debug)]
struct Rendered {
    filter: TopicFilter,
    first: Option<LevelKey>,
}

impl Rendered {
    fn new(filter: TopicFilter) -> Self {
        let first = first_key(filter.pattern());
        Self { filter, first }
    }
}

/// A topic's first level, packed into 64 bits: its length and up to seven of its bytes. Two
/// levels with different keys differ, so a rule's topic whose first level is a literal is ruled
/// out by one comparison; two with the same key are then compared in full.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct LevelKey(u64);

impl LevelKey {
    fn of(level: &str) -> Self {
        let bytes = level.as_bytes();
        let length = u64::from(u8::try_from(bytes.len()).unwrap_or(u8::MAX));
        let mut key = length << 56;
        for (i, &byte) in bytes.iter().take(7).enumerate() {
            key |= u64::from(byte) << (i * 8);
        }
        Self(key)
    }
}

/// The key of a filter's first level, `None` when it is a wildcard and matches any.
fn first_key(pattern: &str) -> Option<LevelKey> {
    let first = pattern.split('/').next().unwrap_or(pattern);
    (first != "+" && first != "#").then(|| LevelKey::of(first))
}

/// What an action is checked against: a topic name, or the filter of a subscription without
/// its `$share/{ShareName}/`.
#[derive(Clone, Copy)]
enum Target<'a> {
    Name(&'a TopicName),
    Filter(&'a str),
}

/// The bit of an action, what it is checked against, and the key of that's first level. Bits 0
/// to 5 are a publish by QoS and RETAIN, bits 6 to 8 a subscription or a delivery by QoS.
/// `None` for an action this version does not know, which is denied.
fn request<'a>(action: &Action<'a>) -> Option<(u16, Target<'a>, LevelKey)> {
    let (bit, target, text) = match *action {
        Action::Publish {
            topic, qos, retain, ..
        } => (
            publish_bit(qos, retain),
            Target::Name(topic),
            topic.as_str(),
        ),
        // A delivery is allowed when the client could subscribe to its topic name.
        Action::Receive { topic, qos, .. } => {
            (subscribe_bit(qos), Target::Name(topic), topic.as_str())
        }
        Action::Subscribe { filter, qos, .. } => {
            let pattern = filter.pattern();
            (subscribe_bit(qos), Target::Filter(pattern), pattern)
        }
        _ => return None,
    };
    let first = text.split('/').next().unwrap_or(text);
    Some((bit, target, LevelKey::of(first)))
}

const fn publish_bit(qos: QoS, retain: bool) -> u16 {
    1 << (qos.value() * 2 + retain as u8)
}

const fn subscribe_bit(qos: QoS) -> u16 {
    1 << (6 + qos.value())
}

fn actions(spec: &RuleSpec) -> u16 {
    let mut bits = 0;
    for qos in spec.qos.iter() {
        if spec.action.publishes() {
            for retain in [false, true] {
                if spec.retain.is_none_or(|wanted| wanted == retain) {
                    bits |= publish_bit(qos, retain);
                }
            }
        }
        if spec.action.subscribes() {
            bits |= subscribe_bit(qos);
        }
    }
    bits
}

impl Acl {
    /// Compiles `rules`, in order.
    ///
    /// # Errors
    ///
    /// The problems, each with the number of its rule counted from 1, in [`Error::Acl`]: see
    /// `docs/spec/acl.md` for what each part of a rule accepts.
    pub fn compile(rules: &[RuleSpec]) -> Result<Self, Error> {
        Self::compile_with(rules, &|index| format!("rule {}", index + 1))
    }

    /// [`compile`](Self::compile), naming each rule as `name` says.
    pub(crate) fn compile_with(
        rules: &[RuleSpec],
        name: &dyn Fn(usize) -> String,
    ) -> Result<Self, Error> {
        let mut problems = Vec::new();
        let mut compiled = Vec::with_capacity(rules.len());
        let mut templates = 0;
        for (index, spec) in rules.iter().enumerate() {
            match compile_rule(spec, &mut templates) {
                Ok(rule) => compiled.push(rule),
                Err(found) => problems.extend(
                    found
                        .into_iter()
                        .map(|problem| format!("{}: {problem}", name(index))),
                ),
            }
        }
        if !problems.is_empty() {
            return Err(Error::Acl { problems });
        }
        Ok(Self {
            rules: compiled.into_boxed_slice(),
        })
    }

    /// No rules: every action is denied, as when `auth.acl_file` is unset.
    pub fn empty() -> Self {
        Self {
            rules: Box::new([]),
        }
    }

    /// How many rules there are.
    pub fn len(&self) -> usize {
        self.rules.len()
    }

    /// Whether there are none.
    pub fn is_empty(&self) -> bool {
        self.rules.is_empty()
    }

    /// Decides whether `client` may do `action`, from scratch: every rule's conditions are
    /// evaluated for this call. A connection [binds](Self::bind) instead.
    pub fn decide(&self, client: &ClientInfo, action: &Action<'_>) -> Permission {
        let Some((bit, target, first)) = request(action) else {
            return Permission::Deny;
        };
        let username = client.username().map(|name| name.as_str());
        let client_id = client.client_id.as_str();
        for rule in &*self.rules {
            if rule.actions & bit == 0 || !who_matches(&rule.who, client) {
                continue;
            }
            let matched = rule.topics.iter().any(|topic| match topic {
                Topic::Template { template, .. } => template
                    .render(username, client_id)
                    .is_some_and(|filter| filter_matches(&filter, target, rule.decision)),
                other => topic_matches(other, None, target, first, rule.decision),
            });
            if matched {
                return permission(rule.decision);
            }
        }
        Permission::Deny
    }

    /// The rules for `client`, specialised once: which rules apply to it, and its own values
    /// in place of each placeholder.
    pub fn bind(self: &Arc<Self>, client: &ClientInfo) -> ClientRules {
        let words = self.rules.len().div_ceil(64);
        let mut applicable = vec![0u64; words].into_boxed_slice();
        let mut rendered = Vec::new();
        let username = client.username().map(|name| name.as_str());
        let client_id = client.client_id.as_str();
        for (index, rule) in self.rules.iter().enumerate() {
            if !who_matches(&rule.who, client) {
                continue;
            }
            applicable[index / 64] |= 1 << (index % 64);
            for topic in &*rule.topics {
                if let Topic::Template { template, slot } = topic
                    && let Some(filter) = template.render(username, client_id)
                {
                    rendered.push((*slot, Rendered::new(filter)));
                }
            }
        }
        ClientRules {
            acl: Arc::clone(self),
            applicable,
            rendered: rendered.into_boxed_slice(),
        }
    }
}

fn permission(decision: Decision) -> Permission {
    match decision {
        Decision::Allow => Permission::Allow,
        Decision::Deny => Permission::Deny,
    }
}

/// Whether `filter`, of a rule that decides `decision`, matches `target`. A subscription is
/// held to an allow rule's filter level by level (R2 rule 11), and refused by a deny rule's
/// filter wherever 1.x refused it.
fn filter_matches(filter: &TopicFilter, target: Target<'_>, decision: Decision) -> bool {
    match target {
        Target::Name(name) => filter.matches(name),
        Target::Filter(pattern) => match decision {
            Decision::Allow => covers(filter.pattern(), pattern),
            Decision::Deny => denies(filter.pattern(), pattern),
        },
    }
}

/// Whether `topic`, of a rule that decides `decision`, matches `target`, whose first level's
/// key is `first`; a template's filter is `rendered`.
fn topic_matches(
    topic: &Topic,
    rendered: Option<&Rendered>,
    target: Target<'_>,
    first: LevelKey,
    decision: Decision,
) -> bool {
    let differs = |key: Option<LevelKey>| key.is_some_and(|key| key != first);
    match topic {
        Topic::Filter { filter, first: key } => {
            !differs(*key) && filter_matches(filter, target, decision)
        }
        Topic::Template { .. } => rendered.is_some_and(|rendered| {
            !differs(rendered.first) && filter_matches(&rendered.filter, target, decision)
        }),
        Topic::Exact { text, first: key } => {
            *key == first
                && match target {
                    Target::Name(name) => name.as_str() == &**text,
                    Target::Filter(pattern) => pattern == &**text,
                }
        }
        Topic::All => true,
    }
}

fn who_matches(conditions: &[Condition], client: &ClientInfo) -> bool {
    conditions.iter().all(|condition| match condition {
        Condition::Username(matchers) => client
            .username()
            .is_some_and(|name| any_matches(matchers, name.as_str())),
        Condition::ClientId(matchers) => any_matches(matchers, client.client_id.as_str()),
        Condition::Address(networks) => client.address.is_some_and(|address| {
            let address = canonical(address.ip());
            networks.iter().any(|network| network.contains(address))
        }),
        Condition::Attribute(name, matchers) => client
            .principal
            .attributes
            .get(name)
            .is_some_and(|value| any_matches(matchers, value)),
    })
}

fn any_matches(matchers: &[Matcher], value: &str) -> bool {
    matchers.iter().any(|matcher| match matcher {
        Matcher::Exact(exact) => value == &**exact,
        Matcher::Prefix(prefix) => value.starts_with(&**prefix),
        Matcher::Regex(regex) => regex.is_match(value),
    })
}

fn compile_rule(spec: &RuleSpec, templates: &mut usize) -> Result<Rule, Vec<String>> {
    let mut problems = Vec::new();
    if spec.decision == Decision::Allow && spec.who.address.is_some() && !spec.who.names_identity()
    {
        problems.push(
            "an allow rule cannot match on the client's address alone, which grants nothing by \
             itself (R2 rule 14): add `username`, `client_id` or `attributes`"
                .to_owned(),
        );
    }
    if spec.qos.is_empty() {
        problems.push("`qos` lists no level, so the rule matches nothing".to_owned());
    }
    if spec.retain.is_some() && !spec.action.publishes() {
        problems.push("`retain` applies to publishes, and the action is `subscribe`".to_owned());
    }
    let who = compile_who(&spec.who, &mut problems);
    if spec.topics.is_empty() {
        problems.push("`topics` is empty, so the rule matches nothing".to_owned());
    }
    let mut topics = Vec::with_capacity(spec.topics.len());
    for topic in &spec.topics {
        match compile_topic(topic, templates) {
            Ok(topic) => topics.push(topic),
            Err(problem) => problems.push(problem),
        }
    }
    if !problems.is_empty() {
        return Err(problems);
    }
    Ok(Rule {
        decision: spec.decision,
        who: who.into_boxed_slice(),
        actions: actions(spec),
        topics: topics.into_boxed_slice(),
    })
}

fn compile_who(who: &Who, problems: &mut Vec<String>) -> Vec<Condition> {
    let mut conditions = Vec::new();
    if let Some(matchers) = &who.username {
        conditions.push(Condition::Username(compile_matchers(
            "username", matchers, problems,
        )));
    }
    if let Some(matchers) = &who.client_id {
        conditions.push(Condition::ClientId(compile_matchers(
            "client_id",
            matchers,
            problems,
        )));
    }
    if let Some(networks) = &who.address {
        if networks.is_empty() {
            problems.push("`address` lists no network, so the rule matches nobody".to_owned());
        }
        conditions.push(Condition::Address(networks.clone().into_boxed_slice()));
    }
    for (name, matchers) in &who.attributes {
        if name.is_empty() {
            problems.push("an attribute's name is empty".to_owned());
        }
        let key = format!("attributes.{name}");
        conditions.push(Condition::Attribute(
            name.as_str().into(),
            compile_matchers(&key, matchers, problems),
        ));
    }
    conditions
}

fn compile_matchers(
    key: &str,
    matchers: &[NameMatch],
    problems: &mut Vec<String>,
) -> Box<[Matcher]> {
    if matchers.is_empty() {
        problems.push(format!("`{key}` lists nothing, so the rule matches nobody"));
    }
    let mut compiled = Vec::with_capacity(matchers.len());
    for matcher in matchers {
        match matcher {
            NameMatch::Exact(exact) => compiled.push(Matcher::Exact(exact.as_str().into())),
            NameMatch::Prefix(prefix) if prefix.is_empty() => problems.push(format!(
                "`{key}` has an empty prefix, which matches everyone: leave the key out instead"
            )),
            NameMatch::Prefix(prefix) => compiled.push(Matcher::Prefix(prefix.as_str().into())),
            NameMatch::Regex(pattern) => {
                // Anchored, so the pattern has to match the whole name: an unanchored `dev`
                // would match `not-a-dev` too.
                match RegexBuilder::new(&format!("^(?:{pattern})$"))
                    .size_limit(REGEX_SIZE_LIMIT)
                    .build()
                {
                    Ok(regex) => compiled.push(Matcher::Regex(regex)),
                    Err(error) => problems.push(format!(
                        "`{key}` has a regular expression that does not compile: {}",
                        first_line(&error.to_string())
                    )),
                }
            }
        }
    }
    compiled.into_boxed_slice()
}

fn first_line(text: &str) -> &str {
    text.lines()
        .find(|line| !line.trim().is_empty())
        .unwrap_or(text)
        .trim()
}

/// Whether `topic` is one a rule can hold, and why not: for the converter, which leaves out a
/// topic that matches nothing.
pub(crate) fn check_topic(topic: &TopicSpec) -> Result<(), String> {
    compile_topic(topic, &mut 0).map(|_| ())
}

fn compile_topic(topic: &TopicSpec, templates: &mut usize) -> Result<Topic, String> {
    match topic {
        TopicSpec::All => Ok(Topic::All),
        TopicSpec::Exact(text) => {
            if text.is_empty() || text.len() > MAX_TOPIC_LEN || text.contains('\0') {
                return Err(format!("`{{ eq = {text:?} }}` is not a topic or a filter"));
            }
            if text.starts_with("$share/") {
                return Err(format!(
                    "`{{ eq = {text:?} }}` names a shared subscription; a subscription is \
                     checked without its `$share/{{ShareName}}/`"
                ));
            }
            let level = text.split('/').next().unwrap_or(text);
            Ok(Topic::Exact {
                text: text.as_str().into(),
                first: LevelKey::of(level),
            })
        }
        TopicSpec::Filter(text) => {
            let template = Template::parse(text).map_err(|error| match error {
                TemplateError::Unclosed => {
                    format!("the topic {text:?} opens `${{` and does not close it")
                }
                TemplateError::Unknown(name) => format!(
                    "the topic {text:?} uses ${{{name}}}; a topic may use ${{username}} and \
                     ${{clientid}}"
                ),
            })?;
            if text.starts_with("$share/") {
                return Err(format!(
                    "the topic {text:?} is a shared subscription; a subscription is checked \
                     without its `$share/{{ShareName}}/`"
                ));
            }
            // The text around the placeholders must make a filter whatever a client's values,
            // so try one: an ordinary level for each.
            let sample = template
                .render(Some("u"), "c")
                .ok_or_else(|| format!("the topic {text:?} is not a topic filter"))?;
            if template.has_placeholders() {
                let slot = *templates;
                *templates += 1;
                Ok(Topic::Template { template, slot })
            } else {
                let first = first_key(sample.pattern());
                Ok(Topic::Filter {
                    filter: sample,
                    first,
                })
            }
        }
    }
}

/// An [`Acl`] specialised to one client by [`Acl::bind`].
#[derive(Debug)]
pub struct ClientRules {
    acl: Arc<Acl>,
    /// One bit for each rule, set when the rule applies to this client.
    applicable: Box<[u64]>,
    /// The client's filter for each topic with placeholders in a rule that applies to it, by
    /// the topic's slot, in order. A slot missing has no filter for this client.
    rendered: Box<[(usize, Rendered)]>,
}

impl ClientRules {
    /// Decides whether the client may do `action`.
    pub fn decide(&self, action: &Action<'_>) -> Permission {
        let Some((bit, target, first)) = request(action) else {
            return Permission::Deny;
        };
        // Slots are numbered in rule order, so the rendered filters are met in the order they
        // are kept, and a cursor finds each without a search.
        let mut cursor = 0;
        for (index, rule) in self.acl.rules.iter().enumerate() {
            if rule.actions & bit == 0 || self.applicable[index / 64] & (1 << (index % 64)) == 0 {
                continue;
            }
            for topic in &*rule.topics {
                let rendered = match topic {
                    Topic::Template { slot, .. } => {
                        while self.rendered.get(cursor).is_some_and(|(at, _)| at < slot) {
                            cursor += 1;
                        }
                        self.rendered
                            .get(cursor)
                            .filter(|(at, _)| at == slot)
                            .map(|(_, rendered)| rendered)
                    }
                    _ => None,
                };
                if topic_matches(topic, rendered, target, first, rule.decision) {
                    return permission(rule.decision);
                }
            }
        }
        Permission::Deny
    }

    /// How many rules apply to the client.
    pub fn applicable(&self) -> usize {
        self.applicable
            .iter()
            .map(|word| word.count_ones() as usize)
            .sum()
    }
}

/// An [`Authorizer`] over an [`Acl`] that can be replaced while clients are connected: each
/// bound client notices the new rules at its next decision and binds again.
#[derive(Debug)]
pub struct AclAuthorizer {
    acl: RwLock<Arc<Acl>>,
    /// Raised after every replacement, so a bound client knows its rules are old.
    generation: AtomicU64,
}

impl AclAuthorizer {
    /// Decides by `acl`.
    pub fn new(acl: Acl) -> Self {
        Self {
            acl: RwLock::new(Arc::new(acl)),
            generation: AtomicU64::new(0),
        }
    }

    /// The rules in use.
    pub fn acl(&self) -> Arc<Acl> {
        Arc::clone(&self.acl.read().unwrap_or_else(PoisonError::into_inner))
    }

    /// Decides by `acl` from now on.
    pub fn replace(&self, acl: Acl) {
        *self.acl.write().unwrap_or_else(PoisonError::into_inner) = Arc::new(acl);
        self.generation.fetch_add(1, Ordering::Release);
    }
}

impl Authorizer for AclAuthorizer {
    fn authorize(&self, client: &ClientInfo, action: &Action<'_>) -> Permission {
        self.acl().decide(client, action)
    }

    fn bind(self: Arc<Self>, client: ClientInfo) -> Box<dyn ClientAuthorizer> {
        let generation = self.generation.load(Ordering::Acquire);
        let rules = self.acl().bind(&client);
        Box::new(BoundAcl {
            owner: self,
            client,
            rules: RwLock::new((generation, rules)),
        })
    }
}

struct BoundAcl {
    owner: Arc<AclAuthorizer>,
    client: ClientInfo,
    /// The rules bound, and the generation they were bound at.
    rules: RwLock<(u64, ClientRules)>,
}

impl ClientAuthorizer for BoundAcl {
    fn client(&self) -> &ClientInfo {
        &self.client
    }

    fn authorize(&self, action: &Action<'_>) -> Permission {
        let generation = self.owner.generation.load(Ordering::Acquire);
        {
            let held = self.rules.read().unwrap_or_else(PoisonError::into_inner);
            if held.0 == generation {
                return held.1.decide(action);
            }
        }
        // The rules were replaced since this client bound: bind again, once.
        let rules = self.owner.acl().bind(&self.client);
        let permission = rules.decide(action);
        *self.rules.write().unwrap_or_else(PoisonError::into_inner) = (generation, rules);
        permission
    }
}
