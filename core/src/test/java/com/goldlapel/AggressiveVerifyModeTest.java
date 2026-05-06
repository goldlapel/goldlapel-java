package com.goldlapel;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

class AggressiveVerifyModeTest {

    @Test
    void parseRecognisesNamedModes() {
        assertEquals(AggressiveVerifyMode.AUTO, AggressiveVerifyMode.parse("auto"));
        assertEquals(AggressiveVerifyMode.AUTO, AggressiveVerifyMode.parse("AUTO"));
        assertEquals(AggressiveVerifyMode.AUTO, AggressiveVerifyMode.parse("  Auto  "));
        assertEquals(AggressiveVerifyMode.ON, AggressiveVerifyMode.parse("on"));
        assertEquals(AggressiveVerifyMode.ON, AggressiveVerifyMode.parse("ON"));
        assertEquals(AggressiveVerifyMode.OFF, AggressiveVerifyMode.parse("off"));
        assertEquals(AggressiveVerifyMode.OFF, AggressiveVerifyMode.parse("OFF"));
    }

    @Test
    void parseAcceptsBooleanSpellings() {
        assertEquals(AggressiveVerifyMode.ON, AggressiveVerifyMode.parse("true"));
        assertEquals(AggressiveVerifyMode.ON, AggressiveVerifyMode.parse("yes"));
        assertEquals(AggressiveVerifyMode.ON, AggressiveVerifyMode.parse("1"));
        assertEquals(AggressiveVerifyMode.OFF, AggressiveVerifyMode.parse("false"));
        assertEquals(AggressiveVerifyMode.OFF, AggressiveVerifyMode.parse("no"));
        assertEquals(AggressiveVerifyMode.OFF, AggressiveVerifyMode.parse("0"));
    }

    @Test
    void parseReturnsNullForUnknown() {
        assertNull(AggressiveVerifyMode.parse(null));
        assertNull(AggressiveVerifyMode.parse(""));
        assertNull(AggressiveVerifyMode.parse("   "));
        assertNull(AggressiveVerifyMode.parse("maybe"));
        assertNull(AggressiveVerifyMode.parse("aggressive"));
    }
}
