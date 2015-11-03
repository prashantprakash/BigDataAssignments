import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

/**
 * User-defined function to format the genre
 * 
 * @author Prashant Prakash
 *
 */
@Description(name = "Format_Genre", value = "_FUNC_(str, NetID) - Formats the genre", extended = "Example:\n"
		+ "  > SELECT FORMAT_GENRE(author_name) FROM movies m;")
public class FormatGenre extends UDF {
	/**
	 * Function to format the genre
	 * 
	 * @param s
	 *            Text to format
	 * @param NetId
	 *            Net ID to add
	 * @return Formatted text
	 */
	public Text evaluate(Text s, Text NetId) {
		Text output = new Text("");
		if (s != null) {
			try {
				String[] genres = s.toString().split("\\|");

				StringBuilder stringBuilder = new StringBuilder();

				int i = 0;
				for (; i < genres.length - 2; i++)
					stringBuilder.append(i + 1 + ") " + genres[i] + ", ");

				if (i == genres.length - 2)
					stringBuilder.append(i + 1 + ") " + genres[i++] + " & ");

				stringBuilder.append(i + 1 + ") " + genres[i] + " "
						+ NetId.toString() + " :hive");

				output.set(stringBuilder.toString());
			} catch (Exception e) {
				
				output = new Text(s);
			}
		}

		return output;
	}
}