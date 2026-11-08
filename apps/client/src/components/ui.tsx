/**
 * The few building blocks the screens share. Styles use start and end, never left and right, so a
 * right-to-left layout mirrors without a second set of styles (Constitution VIII).
 */
import type { ReactNode } from "react";
import { Pressable, ScrollView, StyleSheet, Text, TextInput, View } from "react-native";

import { isRtl } from "../i18n";

const colors = {
  text: "#1b1f24",
  muted: "#59636e",
  line: "#d1d9e0",
  accent: "#0b5cad",
  accentText: "#ffffff",
  error: "#b42318",
  surface: "#f6f8fa",
};

/** A scrolling screen whose direction follows the language. */
export function Screen({ children }: { children: ReactNode }) {
  return (
    <View testID="screen-root" style={[styles.root, { direction: isRtl() ? "rtl" : "ltr" }]}>
      <ScrollView contentContainerStyle={styles.content}>{children}</ScrollView>
    </View>
  );
}

/** A screen's heading. */
export function Title({ children }: { children: ReactNode }) {
  return (
    <Text accessibilityRole="header" style={styles.title}>
      {children}
    </Text>
  );
}

/** A section heading. */
export function Heading({ children }: { children: ReactNode }) {
  return (
    <Text accessibilityRole="header" style={styles.heading}>
      {children}
    </Text>
  );
}

/** Body text; `muted` for secondary lines. */
export function Body({ children, muted, testID }: { children: ReactNode; muted?: boolean; testID?: string }) {
  return (
    <Text testID={testID} style={muted ? styles.muted : styles.body}>
      {children}
    </Text>
  );
}

/** Items side by side, start to end. */
export function Row({ children, testID }: { children: ReactNode; testID?: string }) {
  return (
    <View testID={testID} style={styles.row}>
      {children}
    </View>
  );
}

/** A bordered block. */
export function Card({ children, testID }: { children: ReactNode; testID?: string }) {
  return (
    <View testID={testID} style={styles.card}>
      {children}
    </View>
  );
}

/** A button; `quiet` for the less important of two. */
export function Button(props: {
  label: string;
  onPress: () => void;
  testID?: string;
  quiet?: boolean;
  disabled?: boolean;
}) {
  return (
    <Pressable
      testID={props.testID}
      accessibilityRole="button"
      accessibilityState={{ disabled: props.disabled === true }}
      disabled={props.disabled}
      onPress={props.onPress}
      style={[styles.button, props.quiet ? styles.buttonQuiet : null]}
    >
      <Text style={props.quiet ? styles.buttonQuietText : styles.buttonText}>{props.label}</Text>
    </Pressable>
  );
}

/** One choice of several; `selected` when chosen. */
export function Chip(props: { label: string; selected: boolean; onPress: () => void; testID?: string }) {
  return (
    <Pressable
      testID={props.testID}
      accessibilityRole="radio"
      accessibilityState={{ selected: props.selected }}
      onPress={props.onPress}
      style={[styles.chip, props.selected ? styles.chipSelected : null]}
    >
      <Text style={props.selected ? styles.buttonText : styles.body}>{props.label}</Text>
    </Pressable>
  );
}

/** A labelled text field. */
export function Field(props: {
  label: string;
  value: string;
  onChange: (value: string) => void;
  testID?: string;
  secure?: boolean;
  email?: boolean;
}) {
  return (
    <View style={styles.field}>
      <Text style={styles.muted}>{props.label}</Text>
      <TextInput
        testID={props.testID}
        accessibilityLabel={props.label}
        value={props.value}
        onChangeText={props.onChange}
        secureTextEntry={props.secure}
        autoCapitalize="none"
        autoCorrect={false}
        keyboardType={props.email ? "email-address" : "default"}
        style={styles.input}
      />
    </View>
  );
}

/** A refusal or failure, one line per text, read out to screen readers. */
export function ErrorText({ lines }: { lines: string[] }) {
  return (
    <View testID="error-message" accessibilityRole="alert" style={styles.error}>
      {lines.map((line) => (
        <Text key={line} style={styles.errorText}>
          {line}
        </Text>
      ))}
    </View>
  );
}

const styles = StyleSheet.create({
  root: { flex: 1, backgroundColor: "transparent" },
  content: { padding: 16, gap: 12, maxWidth: 720, width: "100%", alignSelf: "center" },
  title: { fontSize: 24, fontWeight: "600", color: colors.text, textAlign: "auto" },
  heading: { fontSize: 18, fontWeight: "600", color: colors.text, marginTop: 8 },
  body: { fontSize: 16, color: colors.text },
  muted: { fontSize: 14, color: colors.muted },
  row: { flexDirection: "row", flexWrap: "wrap", gap: 8, alignItems: "center" },
  card: { borderWidth: 1, borderColor: colors.line, borderRadius: 8, padding: 12, gap: 6 },
  button: {
    backgroundColor: colors.accent,
    borderRadius: 6,
    paddingVertical: 10,
    paddingHorizontal: 16,
    alignSelf: "flex-start",
  },
  buttonQuiet: { backgroundColor: colors.surface, borderWidth: 1, borderColor: colors.line },
  buttonText: { color: colors.accentText, fontSize: 16 },
  buttonQuietText: { color: colors.text, fontSize: 16 },
  chip: {
    borderWidth: 1,
    borderColor: colors.line,
    borderRadius: 16,
    paddingVertical: 6,
    paddingHorizontal: 12,
  },
  chipSelected: { backgroundColor: colors.accent, borderColor: colors.accent },
  field: { gap: 4 },
  input: {
    borderWidth: 1,
    borderColor: colors.line,
    borderRadius: 6,
    paddingVertical: 8,
    paddingHorizontal: 10,
    fontSize: 16,
    color: colors.text,
  },
  error: { borderStartWidth: 4, borderStartColor: colors.error, paddingStart: 8, gap: 2 },
  errorText: { fontSize: 15, color: colors.error },
});
