/**
 * A labelled text field (specs/007-ux-redesign FR-045, FR-063, FR-064).
 *
 * Label above, a 1 px strong border, 44 px tall at comfortable density (36 at compact), helper and
 * error text below. The error is announced and linked to the input; the border and the text carry
 * it together. Disabled fields stay legible; read-only ones look like text. Left-to-right content
 * (e-mail, times, phone numbers) can be forced with `direction`.
 */
import { forwardRef, useId, useState, type ComponentRef, type ReactNode } from "react";
import { useTranslation } from "react-i18next";
import { StyleSheet, TextInput, View, type StyleProp, type TextInputProps, type ViewStyle } from "react-native";

import { isRtlLanguage } from "../../i18n";
import { useKeyboardMode } from "../focus";
import { useTheme } from "../theme/ThemeProvider";
import { textStyle } from "../theme/typography";
import { component } from "../theme/theme.generated";
import { webProps } from "../web";
import { focusRing } from "./Button";
import { Txt } from "./Text";

export interface TextFieldProps extends Omit<TextInputProps, "style" | "editable"> {
  label: string;
  /** Shown under the field when there is no error. */
  helper?: string;
  /** The field's error; shown, announced and linked to the input. */
  error?: string | null;
  /** The error text's test id, when not `<testID>-error` (the admin forms' `field-error-<name>`). */
  errorTestID?: string;
  disabled?: boolean;
  disabledReason?: string;
  readOnly?: boolean;
  /** Force the content's direction (`ltr` for e-mail, phone numbers, times). */
  direction?: "ltr" | "rtl";
  /** A control at the trailing end, inside the border (a calendar button, a clear button). */
  trailing?: ReactNode;
  /** Hide the visible label (it stays for assistive technology). */
  hideLabel?: boolean;
  testID?: string;
  containerStyle?: StyleProp<ViewStyle>;
}

/** The input behind a `TextField`, for focusing it. */
export type TextFieldHandle = ComponentRef<typeof TextInput>;

/** A text field. */
export const TextField = forwardRef<TextFieldHandle, TextFieldProps>(function TextField(
  {
    label,
    helper,
    error,
    errorTestID,
    disabled = false,
    disabledReason,
    readOnly = false,
    direction,
    trailing,
    hideLabel = false,
    testID,
    containerStyle,
    onFocus,
    onBlur,
    ...input
  },
  ref,
) {
  const { colors, density, isDesktop } = useTheme();
  const keyboard = useKeyboardMode();
  const { i18n } = useTranslation();
  const [focused, setFocused] = useState(false);
  const id = useId();
  const labelId = `${id}-label`;
  const noteId = `${id}-note`;
  const height = density === "compact" && isDesktop ? 36 : component.target.phone;
  const hasError = !!error;
  const borderColor = hasError
    ? colors.status.danger
    : focused
      ? colors.accent.default
      : disabled
        ? colors.border.subtle
        : colors.border.strong;
  const type = textStyle("body", { language: i18n.language, desktop: isDesktop });
  const note = hasError ? error : (disabled && disabledReason) || helper;
  const inputWebProps = webProps({
    "aria-labelledby": labelId,
    "aria-describedby": note ? noteId : undefined,
    "aria-invalid": hasError ? "true" : undefined,
    dir: direction,
  });
  const noteWebProps = webProps({ "aria-live": hasError ? "assertive" : undefined });

  return (
    <View style={[styles.root, containerStyle]} testID={testID ? `${testID}-field` : undefined}>
      <Txt
        role="meta"
        tone="secondary"
        nativeID={labelId}
        style={hideLabel ? styles.hidden : null}
        testID={testID ? `${testID}-label` : undefined}
      >
        {label}
      </Txt>
      <View
        style={[
          styles.frame,
          { borderColor, minHeight: height },
          readOnly ? styles.readOnly : null,
          focused && keyboard ? focusRing(colors.focus.ring) : null,
          focused ? { borderWidth: 2 } : null,
        ]}
      >
        <TextInput
          ref={ref}
          {...input}
          testID={testID}
          editable={!disabled && !readOnly}
          readOnly={readOnly}
          accessibilityLabel={label}
          accessibilityState={{ disabled }}
          accessibilityHint={note ?? undefined}
          aria-disabled={disabled}
          {...inputWebProps}
          placeholderTextColor={colors.text.secondary}
          onFocus={(event) => {
            setFocused(true);
            onFocus?.(event);
          }}
          onBlur={(event) => {
            setFocused(false);
            onBlur?.(event);
          }}
          style={[
            styles.input,
            type,
            { color: disabled ? colors.text.secondary : colors.text.primary, minHeight: height - 2 },
            direction ? { writingDirection: direction, textAlign: direction === "ltr" ? "left" : "right" } : null,
            direction && isRtlLanguage(i18n.language) === (direction === "rtl") ? { textAlign: undefined } : null,
          ]}
        />
        {trailing ? <View style={styles.trailing}>{trailing}</View> : null}
      </View>
      {note ? (
        <Txt
          role="meta"
          tone={hasError ? "danger" : "secondary"}
          nativeID={noteId}
          a11yRole={hasError ? "alert" : undefined}
          {...noteWebProps}
          testID={hasError && errorTestID ? errorTestID : testID ? `${testID}-${hasError ? "error" : "helper"}` : undefined}
        >
          {note}
        </Txt>
      ) : null}
    </View>
  );
});

const styles = StyleSheet.create({
  root: { gap: 6, alignSelf: "stretch" },
  frame: { flexDirection: "row", alignItems: "center", borderWidth: 1, backgroundColor: "transparent" },
  readOnly: { borderColor: "transparent", borderWidth: 1 },
  // minWidth 0: a web input otherwise keeps its default width and pushes the trailing button out (FR-079).
  input: { flex: 1, minWidth: 0, paddingHorizontal: 12, paddingVertical: 0 },
  trailing: { paddingEnd: 4, alignSelf: "stretch", justifyContent: "center" },
  hidden: { position: "absolute", width: 1, height: 1, overflow: "hidden", opacity: 0 },
});
