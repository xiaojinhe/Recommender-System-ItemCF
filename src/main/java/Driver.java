
public class Driver {
	public static void main(String[] args) throws Exception {
		
		DataDividerByUser dataDividerByUser = new DataDividerByUser();
		CoOccurrenceMatrixGenerator coOccurrenceMatrixGenerator = new CoOccurrenceMatrixGenerator();
		Multiplication multiplication = new Multiplication();
		Sum sum = new Sum();
		TopKRecommenderListGenerator topKRecommenderListGenerator = new TopKRecommenderListGenerator();

		String rawInput = args[0];
		String userMovieListOutputDir = args[1];
		String coOccurrenceMatrixDir = args[2];
		String multiplicationDir = args[3];
		String sumDir = args[4];
		String topKDir = args[5];
		String k = args[6];

		String[] path1 = {rawInput, userMovieListOutputDir};
		String[] path2 = {userMovieListOutputDir, coOccurrenceMatrixDir};
		String[] path3 = {coOccurrenceMatrixDir, rawInput, multiplicationDir, userMovieListOutputDir};
		String[] path4 = {multiplicationDir, rawInput, sumDir};
		String[] path5 = {sumDir, topKDir, k};
		
		dataDividerByUser.main(path1);
		coOccurrenceMatrixGenerator.main(path2);
		multiplication.main(path3);
		sum.main(path4);
		topKRecommenderListGenerator.main(path5);
	}

}
