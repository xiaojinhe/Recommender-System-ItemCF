class PredicateData implements Comparable<PredicateData> {
    private String movie;
    private double predicate;

    PredicateData(String movie, double predicate) {
        this.movie = movie;
        this.predicate = predicate;
    }

    public String getMovie() {
        return this.movie;
    }

    public double getPredicate() {
        return this.predicate;
    }

    public int compareTo(PredicateData o) {
        return Double.compare(this.predicate, o.predicate);
    }
}
