package io.boomerang.eventing.nats.jetstream;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

/**
 * Unit test for Subject Match Checker.
 */
public class SubjectMatchCheckerTest {

  @Test
  void testFixedTokens() {

    assertTrue(SubjectMatchChecker.doSubjectsMatch("", ""));
    assertTrue(SubjectMatchChecker.doSubjectsMatch("flow", "flow"));
    assertTrue(SubjectMatchChecker.doSubjectsMatch("flow.event", "flow.event"));
    assertTrue(SubjectMatchChecker.doSubjectsMatch("flow.event.test.lib", "flow.event.test.lib"));

    assertFalse(SubjectMatchChecker.doSubjectsMatch("", "flow"));
    assertFalse(SubjectMatchChecker.doSubjectsMatch("flow", ""));
    assertFalse(SubjectMatchChecker.doSubjectsMatch("flow", "test"));
    assertFalse(SubjectMatchChecker.doSubjectsMatch("flow.event", "flow.test"));
    assertFalse(SubjectMatchChecker.doSubjectsMatch("flow.event.test.lib", "flow.event.test.unknown"));
    assertFalse(SubjectMatchChecker.doSubjectsMatch("flow.event.test.lib", "core.event.test.lib"));
  }

  @Test
  void singleTokenMatch() {

    assertTrue(SubjectMatchChecker.doSubjectsMatch("flow", "*"));
    assertTrue(SubjectMatchChecker.doSubjectsMatch("flow.event", "flow.*"));
    assertTrue(SubjectMatchChecker.doSubjectsMatch("flow.event.one", "*.*.one"));
    assertTrue(SubjectMatchChecker.doSubjectsMatch("flow.event.test.lib", "flow.event.test.*"));
    assertTrue(SubjectMatchChecker.doSubjectsMatch("flow.event.test.lib", "flow.*.test.lib"));
    assertTrue(SubjectMatchChecker.doSubjectsMatch("flow.event.test.lib", "flow.*.*.*"));
    assertTrue(SubjectMatchChecker.doSubjectsMatch("flow.event.test.lib.oops", "flow.*.*.*.oops"));

    assertFalse(SubjectMatchChecker.doSubjectsMatch("", "*"));
    assertFalse(SubjectMatchChecker.doSubjectsMatch("flow", "flow.*"));
    assertFalse(SubjectMatchChecker.doSubjectsMatch("flow.event", "*.flow"));
    assertFalse(SubjectMatchChecker.doSubjectsMatch("flow.event", "*.flow.event"));
    assertFalse(SubjectMatchChecker.doSubjectsMatch("flow.event.one", "*.*.*.one"));
    assertFalse(SubjectMatchChecker.doSubjectsMatch("flow.event", "flow.event.*"));
    assertFalse(SubjectMatchChecker.doSubjectsMatch("flow.event.test.lib", "flow.event.*.test.lib"));
    assertFalse(SubjectMatchChecker.doSubjectsMatch("flow.event.test.lib", "flow.*.*.*.oops"));
  }

  @Test
  void multipleTokensMatch() {

    assertTrue(SubjectMatchChecker.doSubjectsMatch("flow", ">"));
    assertTrue(SubjectMatchChecker.doSubjectsMatch("flow.test", "flow.>"));
    assertTrue(SubjectMatchChecker.doSubjectsMatch("flow.test.one.two", "flow.>"));
    assertTrue(SubjectMatchChecker.doSubjectsMatch("flow.test.one.two.three.four.five", "flow.>.five"));
    assertTrue(SubjectMatchChecker.doSubjectsMatch("flow.test.one.two.three.four.five", ">.five"));
    assertTrue(SubjectMatchChecker.doSubjectsMatch("flow.test.one.two.three.four.five", "flow.>.>.five"));
    assertTrue(SubjectMatchChecker.doSubjectsMatch("flow.test.one.two.three.four.five", "flow.>.>.>.>.>.five"));

    assertFalse(SubjectMatchChecker.doSubjectsMatch("", ">"));
    assertFalse(SubjectMatchChecker.doSubjectsMatch("flow", "flow.>"));
    assertFalse(SubjectMatchChecker.doSubjectsMatch("flow.test", "flow.test.>"));
    assertFalse(SubjectMatchChecker.doSubjectsMatch("flow.five", "flow.>.five"));
    assertFalse(SubjectMatchChecker.doSubjectsMatch("flow.test.one.two.three.four", ">.five"));
    assertFalse(SubjectMatchChecker.doSubjectsMatch("flow.test.one.two.three.four", ">.three"));
    assertFalse(SubjectMatchChecker.doSubjectsMatch("flow.test.one.two.three.four", "flow.>.>.>.>.>.five"));
  }

  @Test
  void mixWildcardMatch() {

    assertTrue(SubjectMatchChecker.doSubjectsMatch("flow.test.one", "flow.*.>"));
    assertTrue(SubjectMatchChecker.doSubjectsMatch("flow.test.one", "flow.>.*"));
    assertTrue(SubjectMatchChecker.doSubjectsMatch("flow.test.one.two", "flow.>.*"));
    assertTrue(SubjectMatchChecker.doSubjectsMatch("flow.test.one.two", "flow.*.>"));
    assertTrue(SubjectMatchChecker.doSubjectsMatch("flow.test.one.two.three.four.five", "flow.>.*.>.five"));
    assertTrue(SubjectMatchChecker.doSubjectsMatch("flow.test.one.two.three.four.five", "flow.>.three.*.five"));
    assertTrue(SubjectMatchChecker.doSubjectsMatch("flow.test.one.two.three.four.five", ">.three.*.five"));
    assertTrue(SubjectMatchChecker.doSubjectsMatch("flow.test.one.two.three.four.five", "*.*.one.>.five"));
    assertTrue(SubjectMatchChecker.doSubjectsMatch("flow.test.one.two.three.four.five", "*.*.one.>"));
    assertTrue(SubjectMatchChecker.doSubjectsMatch("flow.test.one.two.three.four.five", ">.four.*"));
    assertTrue(SubjectMatchChecker.doSubjectsMatch("flow.test.one.two.three.four.five", ">.*.one.>.five"));
    assertTrue(SubjectMatchChecker.doSubjectsMatch("flow.test.one.two.three.four.five", "*.>.>.>.>.>.*"));

    assertFalse(SubjectMatchChecker.doSubjectsMatch("flow.test.one", "flow.*.one.>"));
    assertFalse(SubjectMatchChecker.doSubjectsMatch("flow.test.one", "flow.>.test.*"));
    assertFalse(SubjectMatchChecker.doSubjectsMatch("flow.test.one.two", "test.>.*"));
    assertFalse(SubjectMatchChecker.doSubjectsMatch("flow.test.one.two", "test.*.>"));
    assertFalse(SubjectMatchChecker.doSubjectsMatch("flow.test.one.two.three.four.five", "flow.>.test.>.five"));
    assertFalse(SubjectMatchChecker.doSubjectsMatch("flow.test.one.two.three.four.five", "flow.>.*.three.five"));
    assertFalse(SubjectMatchChecker.doSubjectsMatch("flow.test.one.two.three.four.five", ">.two.*.five"));
    assertFalse(SubjectMatchChecker.doSubjectsMatch("flow.test.one.two.three.four.five", "*.*.two.>.five"));
    assertFalse(SubjectMatchChecker.doSubjectsMatch("flow.test.one.two.three.four.five", "*.*.test.>"));
    assertFalse(SubjectMatchChecker.doSubjectsMatch("flow.test.one.two.three.four.five", ">.three.*"));
    assertFalse(SubjectMatchChecker.doSubjectsMatch("flow.test.one.two.three.four.five", ">.*.four.>.five"));
    assertFalse(SubjectMatchChecker.doSubjectsMatch("flow.test.one.two.three.four.five", "*.*.>.>.>.>.>.*"));
  }
}
