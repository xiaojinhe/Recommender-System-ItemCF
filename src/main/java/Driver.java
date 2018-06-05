/**
 * The Driver set up and run all the jobs for the Recommender System.
 *
 * It takes seven arguments as input:
 * 1) the directory of a raw dataset file -- rawInput
 * 2) the directory of re-format data that divided by user -- userMovieListOutputDir
 * 3) the directory of co-occurrence matrix -- coOccurrenceMatrixDir
 * 4) the directory of unit-multiplication result -- multiplicationDir
 * 5) the directory of overall movie predicates for users -- overallRatingDir
 * 6) the directory of topK recommender list per user --topKDir
 * 7) k for top k recommender list calculation -- k
 *
 */
public class Driver {
	public static void main(String[] args) throws Exception {
		
		DataDividerByUser dataDividerByUser = new DataDividerByUser();
		CoOccurrenceMatrixGenerator coOccurrenceMatrixGenerator = new CoOccurrenceMatrixGenerator();
		Multiplication multiplication = new Multiplication();
		OverallRating overallRating = new OverallRating();
		TopKRecommenderListGenerator topKRecommenderListGenerator = new TopKRecommenderListGenerator();

		String rawInput = args[0];
		String userMovieListOutputDir = args[1];
		String coOccurrenceMatrixDir = args[2];
		String multiplicationDir = args[3];
		String overallRatingDir = args[4];
		String topKDir = args[5];
		String k = args[6];

		String[] path1 = {rawInput, userMovieListOutputDir};
		String[] path2 = {userMovieListOutputDir, coOccurrenceMatrixDir};
		String[] path3 = {coOccurrenceMatrixDir, rawInput, multiplicationDir, userMovieListOutputDir};
		String[] path4 = {multiplicationDir, overallRatingDir};
		String[] path5 = {overallRatingDir, topKDir, k};
		
		dataDividerByUser.main(path1);
		coOccurrenceMatrixGenerator.main(path2);
		multiplication.main(path3);
		overallRating.main(path4);
		topKRecommenderListGenerator.main(path5);
	}

}
