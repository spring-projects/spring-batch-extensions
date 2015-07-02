/**
 * 
 */
package org.springframework.batch.item;

/**
 * @author Jyl-Cristoff
 *
 */
public class Team {
	private String id;
    private String teamName;
    private double creationDate;
    
	public String getId() {
		return id;
	}
	public void setId(String id) {
		this.id = id;
	}
	public String getTeamName() {
		return teamName;
	}
	public void setTeamName(String teamName) {
		this.teamName = teamName;
	}
	public double getCreationDate() {
		return creationDate;
	}
	public void setCreationDate(double creationDate) {
		this.creationDate = creationDate;
	}
	@Override
	public String toString() {
		return "Team [id=" + id + ", teamName=" + teamName + ", creationDate="
				+ creationDate + "]";
	}
}
