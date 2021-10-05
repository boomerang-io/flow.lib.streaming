package io.boomerang.eventing.nats.jetstream;

import java.util.Arrays;

class SubjectMatchChecker {

  static Boolean doSubjectsMatch(String fixedSubject, String wildcardSubject) {

    // @formatter:off
    /**
     * Dynamic programming
     * Time:  O(n * m)
     * Space: O(m)
     * 
     * Example #1
     *      time    us     >  rwt
     * time    1     0     0    0
     *   us    0     1     0    0
     * east    0     0     1    0
     * tree    0     0     1    0
     *    x    0     0     1    0
     *  rwt    0     0     1    1
     * 
     * Example #2
     *      time    us     *  rwt
     * time    1     0     0    0
     *   us    0     1     0    0
     * east    0     0     1    0
     *  rwt    0     0     0    1
     * 
     * Example #3
     *         > east  rwt
     * time    1    0    0
     *   us    1    0    0
     * east    1    1    0
     *  rwt    1    0    1
     */
    // @formatter:on

    // Tokenize strings
    String[] fixedTokens = fixedSubject.split("\\.");
    String[] wildcardTokens = wildcardSubject.split("\\.");

    // Get length of both tokenized strings and create the DP array
    int n = fixedTokens.length, m = wildcardTokens.length;
    boolean[] dp = new boolean[m];

    // Edge case
    if (fixedSubject.length() == 0 || wildcardSubject.length() == 0) {
      return fixedSubject.length() == 0 && wildcardSubject.length() == 0;
    }

    // Dynamic programming for wildcard matching
    for (int i = 0; i < n; i++) {
      boolean[] prev_dp = Arrays.copyOf(dp, m);

      for (int j = 0; j < m; j++) {

        // Get previous DP matrix row values
        boolean prev_diag = j > 0 ? prev_dp[j - 1] : i == 0;
        boolean prev_row = prev_dp[j];

        // Check if both tokens match (considering wildcard)
        boolean match = tokenWildcardMatch(fixedTokens[i], wildcardTokens[j]);

        switch (wildcardTokens[j]) {
          case "*":
            // Wildcard character for matching one token exactly
            dp[j] = prev_diag;
            break;
          case ">":
            // Wildcard character for matching one or more tokens
            dp[j] = prev_diag || prev_row;
            break;
          default:
            // Both tokens must match
            dp[j] = prev_diag && match;
            break;
        }
      }
    }

    return dp[m - 1];
  }

  private static boolean tokenWildcardMatch(String fixedToken, String wildcardToken) {

    if (wildcardToken.equals(">") || wildcardToken.equals("*")) {
      return true;
    }
    return fixedToken.equals(wildcardToken);
  }
}
