package cep.core;

import java.util.ArrayList;
import java.util.List;

/** Global grid cell ids c{row}_{col}, same as poller.py / backfill.py (CONTRACT.md §3.0). */
public final class Grid {
    private Grid() {}

    /** The 8 cells around `cell` (not the cell itself). */
    public static List<String> neighbours(String cell) {
        String[] p = cell.substring(1).split("_");
        int j = Integer.parseInt(p[0]);
        int i = Integer.parseInt(p[1]);
        List<String> out = new ArrayList<>(8);
        for (int dj = -1; dj <= 1; dj++) {
            for (int di = -1; di <= 1; di++) {
                if (dj != 0 || di != 0) out.add("c" + (j + dj) + "_" + (i + di));
            }
        }
        return out;
    }
}
