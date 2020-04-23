package Model;

public class Result {
    public int rowAffected;
    public boolean isInternal = false;

    public Result(int rowAffected) {
        this.rowAffected = rowAffected;
    }

    public Result(int rowAffected, boolean isInternal) {
        this.rowAffected = rowAffected;
        this.isInternal = isInternal;
    }

    public void Display() {
        if(this.isInternal) return;
        System.out.println(String.format("%d rows affected", this.rowAffected));
        System.out.println();
    }
}
