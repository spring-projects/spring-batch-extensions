package org.springframework.batch.item;

import org.springframework.batch.item.excel.mapping.Column;

/**
 * Created by in329dei on 17-9-2014.
 */
public class Player {

	@Column(name = "identificator")
    private String id;
    private String position;
    private String lastName;
    private String firstName;
    private int birthYear;
    private int debutYear;
    private String comment;

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getPosition() {
        return position;
    }

    public void setPosition(String position) {
        this.position = position;
    }

    public String getLastName() {
        return lastName;
    }

    public void setLastName(String lastName) {
        this.lastName = lastName;
    }

    public String getFirstName() {
        return firstName;
    }

    public void setFirstName(String firstName) {
        this.firstName = firstName;
    }

    public int getBirthYear() {
        return birthYear;
    }

    public void setBirthYear(int birthYear) {
        this.birthYear = birthYear;
    }

    public int getDebutYear() {
        return debutYear;
    }
    
    @Column(name = "beginYear")
    public void setDebutYear(int debutYear) {
        this.debutYear = debutYear;
    }

    public String getComment() {
        return comment;
    }

    public void setComment(String comment) {
        this.comment = comment;
    }

	@Override
	public String toString() {
		return "Player [id=" + id + ", position=" + position + ", lastName="
				+ lastName + ", firstName=" + firstName + ", birthYear="
				+ birthYear + ", debutYear=" + debutYear + ", comment="
				+ comment + "]";
	}
}
